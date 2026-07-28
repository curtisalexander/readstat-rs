//! Output writers for converting Arrow [`RecordBatch`] data to CSV, Feather (Arrow IPC),
//! NDJSON, or Parquet format.
//!
//! [`ReadStatWriter`] manages the lifecycle of format-specific writers, handling
//! streaming writes across multiple batches. It also supports metadata output
//! (pretty-printed or JSON), parallel CSV/NDJSON batch encoding, and native
//! parallel Parquet column encoding.

use arrow_array::RecordBatch;
#[cfg(feature = "csv")]
use arrow_csv::WriterBuilder as CsvWriterBuilder;
#[cfg(feature = "feather")]
use arrow_ipc::writer::FileWriter as IpcFileWriter;
#[cfg(feature = "ndjson")]
use arrow_json::LineDelimitedWriter as JsonLineDelimitedWriter;
#[cfg(test)]
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
#[cfg(feature = "parquet")]
use parquet::{
    arrow::{
        ArrowWriter as ParquetArrowWriter,
        arrow_writer::{
            ArrowColumnChunk, ArrowLeafColumn, ArrowRowGroupWriterFactory, compute_leaves,
        },
    },
    basic::Compression as ParquetCompressionCodec,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
};
#[cfg(any(
    feature = "parquet",
    all(any(feature = "csv", feature = "ndjson"), not(target_arch = "wasm32"))
))]
use rayon::prelude::*;
#[cfg(any(
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
use std::fs::File;
#[cfg(any(
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
use std::io::BufWriter;
#[cfg(all(any(feature = "csv", feature = "ndjson"), not(target_arch = "wasm32")))]
use std::io::Write as _;
#[cfg(feature = "csv")]
use std::io::stdout;
#[cfg(any(
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
use std::path::PathBuf;
#[cfg(test)]
use std::sync::Arc;

use crate::err::ReadStatError;
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

#[cfg(any(
    feature = "parquet",
    all(any(feature = "csv", feature = "ndjson"), not(target_arch = "wasm32"))
))]
struct StagingGuard(Option<PathBuf>);

#[cfg(any(
    feature = "parquet",
    all(any(feature = "csv", feature = "ndjson"), not(target_arch = "wasm32"))
))]
impl Drop for StagingGuard {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

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

/// CSV/NDJSON writer that encodes independent batches concurrently and commits
/// their bytes in input order.
///
/// Each call to [`write`](Self::write) is one bounded parallel work group. The
/// caller controls memory by limiting the number and size of batches in that
/// group. CSV emits exactly one header; NDJSON batches require no shared format
/// state.
#[cfg(all(any(feature = "csv", feature = "ndjson"), not(target_arch = "wasm32")))]
pub struct ParallelTextWriter {
    writer: Option<BufWriter<File>>,
    schema: SchemaRef,
    format: OutFormat,
    wrote_batch: bool,
    rows_written: usize,
    staging_path: Option<PathBuf>,
    destination: PathBuf,
    overwrite: bool,
}

#[cfg(all(any(feature = "csv", feature = "ndjson"), not(target_arch = "wasm32")))]
impl ParallelTextWriter {
    /// Creates a transactional parallel CSV or NDJSON writer.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid output configuration or staging-file
    /// creation failure.
    pub fn new(config: WriteConfig, schema: SchemaRef) -> Result<Self, ReadStatError> {
        config.validate()?;
        let supported = match config.format {
            #[cfg(feature = "csv")]
            OutFormat::Csv => true,
            #[cfg(feature = "ndjson")]
            OutFormat::Ndjson => true,
            _ => false,
        };
        if !supported {
            return Err(ReadStatError::InvalidOutputConfig(
                "parallel text writer requires CSV or NDJSON output".into(),
            ));
        }

        let destination = config.out_path.clone().ok_or_else(|| {
            ReadStatError::InvalidOutputConfig(
                "parallel text writer requires an output file".into(),
            )
        })?;
        let (file, staging_path) = crate::rs_write_config::open_output(&config)?;
        let mut staging = StagingGuard(Some(staging_path));

        Ok(Self {
            writer: Some(BufWriter::new(file)),
            schema,
            format: config.format,
            wrote_batch: false,
            rows_written: 0,
            staging_path: staging.0.take(),
            destination,
            overwrite: config.overwrite,
        })
    }

    /// Encodes a bounded group of batches concurrently and writes the encoded
    /// buffers in the same order as `batches`.
    ///
    /// # Errors
    ///
    /// Returns an error for a schema mismatch, row-count overflow, text
    /// encoding failure, or output I/O failure.
    pub fn write(&mut self, batches: &[RecordBatch]) -> Result<(), ReadStatError> {
        if batches.is_empty() {
            return Ok(());
        }
        if batches.iter().any(|batch| batch.schema() != self.schema) {
            return Err(ReadStatError::SchemaMismatch);
        }
        let next_rows = batches.iter().try_fold(self.rows_written, |rows, batch| {
            rows.checked_add(batch.num_rows())
                .ok_or_else(|| ReadStatError::Other("writer row count overflow".into()))
        })?;

        let include_header = !self.wrote_batch && matches!(self.format, OutFormat::Csv);
        let format = self.format;
        let encoded = batches
            .par_iter()
            .enumerate()
            .map(|(index, batch)| encode_text_batch(format, batch, include_header && index == 0))
            .collect::<Result<Vec<_>, _>>()?;

        let write_result = {
            let writer = self
                .writer
                .as_mut()
                .ok_or_else(|| ReadStatError::Other("text writer is already closed".into()))?;
            encoded
                .into_iter()
                .try_for_each(|bytes| writer.write_all(&bytes))
        };
        if let Err(error) = write_result {
            // A failed write may already have modified the staging file. Poison
            // the writer so finish() cannot publish truncated output; Drop
            // removes the staging path.
            self.writer = None;
            return Err(error.into());
        }
        self.wrote_batch = true;
        self.rows_written = next_rows;
        Ok(())
    }

    /// Flushes and atomically publishes the output file. Returns the number of
    /// accepted rows.
    ///
    /// # Errors
    ///
    /// Returns an error if empty-output encoding, flushing, or publication
    /// fails.
    pub fn finish(mut self) -> Result<usize, ReadStatError> {
        if !self.wrote_batch && matches!(self.format, OutFormat::Csv) {
            let empty = RecordBatch::new_empty(self.schema.clone());
            self.write(std::slice::from_ref(&empty))?;
        }
        self.writer
            .take()
            .ok_or_else(|| ReadStatError::Other("text writer is already closed".into()))?
            .flush()?;
        let staging = self
            .staging_path
            .as_ref()
            .expect("parallel text staging path is armed");
        crate::rs_write_config::publish_staging(staging, &self.destination, self.overwrite)?;
        self.staging_path = None;
        Ok(self.rows_written)
    }
}

#[cfg(all(any(feature = "csv", feature = "ndjson"), not(target_arch = "wasm32")))]
impl Drop for ParallelTextWriter {
    fn drop(&mut self) {
        if let Some(path) = self.staging_path.take() {
            self.writer = None;
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(all(any(feature = "csv", feature = "ndjson"), not(target_arch = "wasm32")))]
fn encode_text_batch(
    format: OutFormat,
    batch: &RecordBatch,
    include_header: bool,
) -> Result<Vec<u8>, ReadStatError> {
    #[cfg(not(feature = "csv"))]
    let _ = include_header;
    let mut bytes = Vec::new();
    match format {
        #[cfg(feature = "csv")]
        OutFormat::Csv => {
            let mut writer = CsvWriterBuilder::new()
                .with_header(include_header)
                .build(&mut bytes);
            writer.write(batch)?;
        }
        #[cfg(feature = "ndjson")]
        OutFormat::Ndjson => {
            let mut writer = JsonLineDelimitedWriter::new(&mut bytes);
            writer.write(batch)?;
            writer.finish()?;
        }
        _ => {
            return Err(ReadStatError::InvalidOutputConfig(
                "parallel text writer requires CSV or NDJSON output".into(),
            ));
        }
    }
    Ok(bytes)
}

/// Parquet writer that encodes columns concurrently and commits each row group
/// once, in order, to a single output file.
///
/// Input batches remain ordered and memory is bounded by `row_group_rows` plus
/// upstream buffering. Unlike temporary-file fan-out, encoded pages are copied
/// directly into the final Parquet row group without decoding or re-encoding.
#[cfg(feature = "parquet")]
pub struct ParallelParquetWriter {
    writer: Option<SerializedFileWriter<BufWriter<File>>>,
    factory: ArrowRowGroupWriterFactory,
    schema: SchemaRef,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    row_group_rows: usize,
    row_group_index: usize,
    rows_written: usize,
    staging_path: Option<PathBuf>,
    destination: PathBuf,
    overwrite: bool,
}

#[cfg(feature = "parquet")]
impl ParallelParquetWriter {
    /// Creates a native parallel Parquet writer.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid output configuration, a zero row-group
    /// target, staging-file failures, or invalid Parquet properties.
    pub fn new(
        config: WriteConfig,
        schema: SchemaRef,
        row_group_rows: usize,
    ) -> Result<Self, ReadStatError> {
        config.validate()?;
        if !matches!(config.format, OutFormat::Parquet) {
            return Err(ReadStatError::InvalidOutputConfig(
                "parallel Parquet writer requires Parquet output".into(),
            ));
        }
        if row_group_rows == 0 {
            return Err(ReadStatError::Other(
                "Parquet row-group rows must be greater than zero".into(),
            ));
        }

        let destination = config
            .out_path
            .clone()
            .ok_or_else(|| ReadStatError::InvalidOutputConfig("Parquet requires output".into()))?;
        let compression = crate::rs_write_config::resolve_parquet_compression(
            config.compression,
            config.compression_level,
        )?;
        let (file, staging_path) = crate::rs_write_config::open_output(&config)?;
        let mut staging = StagingGuard(Some(staging_path));
        let properties = WriterProperties::builder()
            .set_compression(compression)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .build();
        let (writer, factory) =
            ParquetArrowWriter::try_new(BufWriter::new(file), schema.clone(), Some(properties))?
                .into_serialized_writer()?;

        Ok(Self {
            writer: Some(writer),
            factory,
            schema,
            pending: Vec::new(),
            pending_rows: 0,
            row_group_rows,
            row_group_index: 0,
            rows_written: 0,
            staging_path: staging.0.take(),
            destination,
            overwrite: config.overwrite,
        })
    }

    /// Queues a batch and flushes complete row groups with parallel column
    /// encoding. Batches crossing a row-group boundary are sliced without
    /// copying their Arrow buffers.
    ///
    /// # Errors
    ///
    /// Returns an error for a schema mismatch, row-count overflow, or Parquet
    /// encoding/write failure.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ReadStatError> {
        if batch.schema() != self.schema {
            return Err(ReadStatError::SchemaMismatch);
        }
        if self.schema.fields().is_empty() && batch.num_rows() != 0 {
            return Err(ReadStatError::Other(
                "Parquet cannot represent rows without columns".into(),
            ));
        }

        let mut offset = 0;
        while offset < batch.num_rows() {
            let available = self.row_group_rows - self.pending_rows;
            let rows = available.min(batch.num_rows() - offset);
            self.pending.push(batch.slice(offset, rows));
            self.pending_rows += rows;
            offset += rows;
            if self.pending_rows == self.row_group_rows {
                self.flush_row_group()?;
            }
        }
        self.rows_written = self
            .rows_written
            .checked_add(batch.num_rows())
            .ok_or_else(|| ReadStatError::Other("writer row count overflow".into()))?;
        Ok(())
    }

    fn flush_row_group(&mut self) -> Result<(), ReadStatError> {
        if self.pending_rows == 0 {
            return Ok(());
        }

        let column_writers = self.factory.create_column_writers(self.row_group_index)?;
        let mut inputs: Vec<Vec<ArrowLeafColumn>> = (0..column_writers.len())
            .map(|_| Vec::with_capacity(self.pending.len()))
            .collect();

        for batch in &self.pending {
            let mut leaf_index = 0;
            for (field, array) in self.schema.fields().iter().zip(batch.columns()) {
                for leaf in compute_leaves(field.as_ref(), array)? {
                    inputs[leaf_index].push(leaf);
                    leaf_index += 1;
                }
            }
            if leaf_index != column_writers.len() {
                return Err(ReadStatError::Other(
                    "computed Parquet leaf count does not match column writers".into(),
                ));
            }
        }

        let chunks: Vec<ArrowColumnChunk> = column_writers
            .into_par_iter()
            .zip(inputs.into_par_iter())
            .map(|(mut writer, leaves)| {
                for leaf in &leaves {
                    writer.write(leaf)?;
                }
                writer.close()
            })
            .collect::<Result<Vec<_>, _>>()?;

        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| ReadStatError::Other("Parquet writer is already closed".into()))?;
        let mut row_group = writer.next_row_group()?;
        for chunk in chunks {
            chunk.append_to_row_group(&mut row_group)?;
        }
        row_group.close()?;

        self.pending.clear();
        self.pending_rows = 0;
        self.row_group_index += 1;
        Ok(())
    }

    /// Flushes the final row group, writes the footer, and atomically publishes
    /// the output file. Returns the number of accepted rows.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding, finalization, or publication fails.
    pub fn finish(mut self) -> Result<usize, ReadStatError> {
        self.flush_row_group()?;
        self.writer
            .take()
            .ok_or_else(|| ReadStatError::Other("Parquet writer is already closed".into()))?
            .close()?;
        let staging = self
            .staging_path
            .as_ref()
            .expect("parallel Parquet staging path is armed");
        crate::rs_write_config::publish_staging(staging, &self.destination, self.overwrite)?;
        self.staging_path = None;
        Ok(self.rows_written)
    }
}

#[cfg(feature = "parquet")]
impl Drop for ParallelParquetWriter {
    fn drop(&mut self) {
        if let Some(path) = self.staging_path.take() {
            self.writer = None;
            let _ = std::fs::remove_file(path);
        }
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
    #[cfg(feature = "csv")]
    pub(crate) wrote_header: bool,
    /// Whether any data has been written (controls file creation vs. append).
    pub(crate) wrote_start: bool,
    config: WriteConfig,
    schema: SchemaRef,
    rows_written: usize,
    #[cfg(any(
        feature = "csv",
        feature = "feather",
        feature = "ndjson",
        feature = "parquet"
    ))]
    staging_path: Option<PathBuf>,
}

impl ReadStatWriter {
    /// Creates a new `ReadStatWriter` with no active writer.
    pub fn new(config: WriteConfig, schema: SchemaRef) -> Result<Self, ReadStatError> {
        config.validate()?;
        Ok(Self {
            wtr: None,
            #[cfg(feature = "csv")]
            wrote_header: false,
            wrote_start: false,
            config,
            schema,
            rows_written: 0,
            #[cfg(any(
                feature = "csv",
                feature = "feather",
                feature = "ndjson",
                feature = "parquet"
            ))]
            staging_path: None,
        })
    }

    /// Opens a sibling staging file. Called exactly once per output; successful
    /// finalization publishes it to the configured destination.
    #[cfg(any(
        feature = "csv",
        feature = "feather",
        feature = "ndjson",
        feature = "parquet"
    ))]
    fn open_output(&mut self, wc: &WriteConfig) -> Result<File, ReadStatError> {
        debug_assert!(!self.wrote_start, "output file opened twice");
        let (file, staging_path) = crate::rs_write_config::open_output(wc)?;
        self.staging_path = Some(staging_path);
        Ok(file)
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
    /// Returns the total number of successfully written rows. Consuming the
    /// writer makes finalization a one-shot operation. The library does not
    /// print anything; callers own user-facing summary output.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails to flush or close,
    /// or if the output format is not enabled.
    #[allow(unused_variables)]
    pub fn finish(mut self) -> Result<usize, ReadStatError> {
        // Initialize even for zero rows, producing a schema-carrying output.
        if !self.wrote_start {
            self.write(&RecordBatch::new_empty(self.schema.clone()))?;
        }
        match self.config.format {
            #[cfg(feature = "csv")]
            OutFormat::Csv => {
                // Explicitly flush: relying on BufWriter's Drop would silently
                // discard I/O errors (e.g. disk full), reporting success over
                // a truncated file.
                self.flush_buffered()?;
                self.publish()
            }
            #[cfg(feature = "feather")]
            OutFormat::Feather => {
                self.finish_feather()?;
                self.publish()
            }
            #[cfg(feature = "ndjson")]
            OutFormat::Ndjson => {
                self.flush_buffered()?;
                self.publish()
            }
            #[cfg(feature = "parquet")]
            OutFormat::Parquet => {
                self.finish_parquet()?;
                self.publish()
            }
            #[allow(unreachable_patterns)]
            _ => Err(ReadStatError::Other(format!(
                "Output format {:?} is not enabled. Enable the corresponding feature flag.",
                self.config.format
            ))),
        }
    }

    #[cfg(any(
        feature = "csv",
        feature = "feather",
        feature = "ndjson",
        feature = "parquet"
    ))]
    fn publish(&mut self) -> Result<usize, ReadStatError> {
        let Some(staging) = self.staging_path.take() else {
            return Ok(self.rows_written);
        };
        // Close every handle before publication (required by Windows too).
        self.wtr = None;
        let destination = self
            .config
            .out_path
            .as_ref()
            .expect("staging has destination");
        let result =
            crate::rs_write_config::publish_staging(&staging, destination, self.config.overwrite);
        if result.is_err() {
            let _ = std::fs::remove_file(&staging);
        }
        result?;
        Ok(self.rows_written)
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
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ReadStatError> {
        if batch.schema() != self.schema {
            return Err(ReadStatError::SchemaMismatch);
        }
        let wc = self.config.clone();
        match wc.format {
            #[cfg(feature = "csv")]
            OutFormat::Csv => {
                if wc.out_path.is_none() {
                    if self.wrote_header {
                        self.write_data_to_stdout(batch)
                    } else {
                        self.write_header_to_stdout()?;
                        self.write_data_to_stdout(batch)
                    }
                } else {
                    self.write_data_to_csv(batch, &wc)
                }
            }
            #[cfg(feature = "feather")]
            OutFormat::Feather => self.write_data_to_feather(batch, &wc),
            #[cfg(feature = "ndjson")]
            OutFormat::Ndjson => self.write_data_to_ndjson(batch, &wc),
            #[cfg(feature = "parquet")]
            OutFormat::Parquet => self.write_data_to_parquet(batch, &wc),
            #[allow(unreachable_patterns)]
            _ => Err(ReadStatError::Other(format!(
                "Output format {:?} is not enabled. Enable the corresponding feature flag.",
                wc.format
            ))),
        }?;
        self.rows_written = self
            .rows_written
            .checked_add(batch.num_rows())
            .ok_or_else(|| ReadStatError::Other("writer row count overflow".into()))?;
        Ok(())
    }

    #[cfg(feature = "csv")]
    fn write_data_to_csv(
        &mut self,
        batch: &RecordBatch,
        wc: &WriteConfig,
    ) -> Result<(), ReadStatError> {
        if wc.out_path.is_some() {
            // Open the file only on the first batch; later batches reuse the
            // open writer. Opening (and immediately dropping) the handle on
            // every batch was wasted syscalls.
            if !self.wrote_start {
                let f = self.open_output(wc)?;
                self.wtr = Some(ReadStatWriterFormat::Csv(BufWriter::new(f)));
            }

            // write
            if let Some(ReadStatWriterFormat::Csv(f)) = &mut self.wtr {
                let include_header = !self.wrote_header;
                let mut writer = CsvWriterBuilder::new().with_header(include_header).build(f);
                writer.write(batch)?;
                self.wrote_header = true;

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
        batch: &RecordBatch,
        wc: &WriteConfig,
    ) -> Result<(), ReadStatError> {
        if wc.out_path.is_some() {
            // Open the file only on the first batch (see write_data_to_csv).
            if !self.wrote_start {
                let f = self.open_output(wc)?;
                let wtr = IpcFileWriter::try_new(BufWriter::new(f), &self.schema)?;
                self.wtr = Some(ReadStatWriterFormat::Feather(wtr));
            }

            // write
            if let Some(ReadStatWriterFormat::Feather(wtr)) = &mut self.wtr {
                wtr.write(batch)?;

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
        batch: &RecordBatch,
        wc: &WriteConfig,
    ) -> Result<(), ReadStatError> {
        if wc.out_path.is_some() {
            // Open the file only on the first batch (see write_data_to_csv).
            if !self.wrote_start {
                let f = self.open_output(wc)?;
                self.wtr = Some(ReadStatWriterFormat::Ndjson(BufWriter::new(f)));
            }

            // write
            if let Some(ReadStatWriterFormat::Ndjson(f)) = &mut self.wtr {
                let mut writer = JsonLineDelimitedWriter::new(f);
                writer.write(batch)?;
                writer.finish()?;

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
        batch: &RecordBatch,
        wc: &WriteConfig,
    ) -> Result<(), ReadStatError> {
        if self.schema.fields().is_empty() && batch.num_rows() != 0 {
            return Err(ReadStatError::Other(
                "Parquet cannot represent rows without columns".into(),
            ));
        }
        if wc.out_path.is_some() {
            // setup writer — open the file only on the first batch (see
            // write_data_to_csv).
            if !self.wrote_start {
                let f = self.open_output(wc)?;
                let compression_codec =
                    Self::resolve_compression(wc.compression, wc.compression_level)?;

                let props = WriterProperties::builder()
                    .set_compression(compression_codec)
                    .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                    .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
                    .build();

                let wtr = ParquetArrowWriter::try_new(
                    BufWriter::new(f),
                    self.schema.clone(),
                    Some(props),
                )?;

                self.wtr = Some(ReadStatWriterFormat::Parquet(ReadStatParquetWriter::new(
                    wtr,
                )));
            }

            // write
            if let Some(ReadStatWriterFormat::Parquet(pwtr)) = &mut self.wtr {
                if let Some(ref mut wtr) = pwtr.wtr {
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
    fn write_data_to_stdout(&mut self, batch: &RecordBatch) -> Result<(), ReadStatError> {
        // writer setup
        if !self.wrote_start {
            self.wtr = Some(ReadStatWriterFormat::CsvStdout(stdout()));
        }

        // write
        if let Some(ReadStatWriterFormat::CsvStdout(f)) = &mut self.wtr {
            let mut writer = CsvWriterBuilder::new().with_header(false).build(f);
            writer.write(batch)?;

            self.wrote_start = true;

            Ok(())
        } else {
            Err(ReadStatError::Other(
                "Error writing to csv as associated writer is not for the csv format".to_string(),
            ))
        }
    }

    #[cfg(feature = "csv")]
    fn write_header_to_stdout(&mut self) -> Result<(), ReadStatError> {
        use std::io::Write;

        // CSV-escape each name so the header stays well-formed and column-aligned
        // with the (already-escaped) data rows. Variable names may legally contain
        // commas or quotes under SAS `VALIDVARNAME=ANY`.
        let header = self
            .schema
            .fields()
            .iter()
            .map(|field| csv_escape_field(field.name()))
            .collect::<Vec<_>>()
            .join(",");

        // writeln! (not println!): a closed pipe (e.g. `... | head`) must
        // surface as an I/O error, not a panic.
        writeln!(stdout(), "{header}")?;

        self.wrote_header = true;

        Ok(())
    }
}

impl Drop for ReadStatWriter {
    fn drop(&mut self) {
        #[cfg(any(
            feature = "csv",
            feature = "feather",
            feature = "ndjson",
            feature = "parquet"
        ))]
        if let Some(path) = self.staging_path.take() {
            // Drop format writers/file handles before attempting cleanup.
            self.wtr = None;
            let _ = std::fs::remove_file(path);
        }
    }
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

/// Serialize a [`RecordBatch`] to CSV bytes (with header).
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

/// Serialize a [`RecordBatch`] to NDJSON bytes.
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

/// Serialize a [`RecordBatch`] to Parquet bytes with Snappy compression.
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

/// Serialize a [`RecordBatch`] to Feather (Arrow IPC) bytes.
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

// These lifecycle tests exercise every writer backend together. Keep the
// module aligned with that contract so minimal-feature test builds remain a
// valid supported configuration.
#[cfg(all(
    test,
    feature = "csv",
    feature = "feather",
    feature = "ndjson",
    feature = "parquet"
))]
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
        let wtr = ReadStatWriter::new(WriteConfig::new(OutFormat::Csv), Arc::new(Schema::empty()))
            .unwrap();
        assert!(wtr.wtr.is_none());
        assert!(!wtr.wrote_header);
        assert!(!wtr.wrote_start);
    }

    fn test_batch(schema: SchemaRef, values: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::StringArray::from(values.to_vec()))],
        )
        .unwrap()
    }

    #[test]
    fn multi_batch_rows_and_consuming_finish() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let config = WriteConfig::new(OutFormat::Csv)
            .output(dir.path().join("rows.csv"))
            .unwrap();
        let mut writer = ReadStatWriter::new(config, schema.clone()).unwrap();
        writer
            .write(&test_batch(schema.clone(), &["a", "b"]))
            .unwrap();
        writer.write(&test_batch(schema.clone(), &["c"])).unwrap();
        assert_eq!(writer.finish().unwrap(), 3);
    }

    #[test]
    fn parallel_csv_matches_serial_bytes_and_row_count() {
        let dir = tempfile::tempdir().unwrap();
        let serial_path = dir.path().join("serial.csv");
        let parallel_path = dir.path().join("parallel.csv");
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let empty = test_batch(schema.clone(), &[]);
        let first = test_batch(schema.clone(), &["a", "b"]);
        let second = test_batch(schema.clone(), &["c"]);

        let mut serial = ReadStatWriter::new(
            WriteConfig::new(OutFormat::Csv)
                .output(&serial_path)
                .unwrap(),
            schema.clone(),
        )
        .unwrap();
        for batch in [&empty, &first, &second] {
            serial.write(batch).unwrap();
        }
        assert_eq!(serial.finish().unwrap(), 3);

        let mut parallel = ParallelTextWriter::new(
            WriteConfig::new(OutFormat::Csv)
                .output(&parallel_path)
                .unwrap(),
            schema,
        )
        .unwrap();
        parallel.write(std::slice::from_ref(&empty)).unwrap();
        parallel.write(std::slice::from_ref(&first)).unwrap();
        parallel.write(std::slice::from_ref(&second)).unwrap();
        assert_eq!(parallel.finish().unwrap(), 3);

        assert_eq!(
            std::fs::read(serial_path).unwrap(),
            std::fs::read(parallel_path).unwrap()
        );
    }

    #[test]
    fn parallel_csv_schema_error_preserves_destination_and_cleans_staging() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("preserve.csv");
        std::fs::write(&path, "sentinel").unwrap();
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let other = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "y",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let config = WriteConfig::new(OutFormat::Csv)
            .output(&path)
            .unwrap()
            .overwrite(true);
        let mut writer = ParallelTextWriter::new(config, schema.clone()).unwrap();
        writer.write(&[test_batch(schema, &["staged"])]).unwrap();
        assert!(matches!(
            writer.write(&[test_batch(other, &["new"])]),
            Err(ReadStatError::SchemaMismatch)
        ));
        drop(writer);

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "sentinel");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn parallel_text_empty_outputs_match_serial() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        for format in [OutFormat::Csv, OutFormat::Ndjson] {
            let serial_path = dir.path().join(format!("serial.{format}"));
            let parallel_path = dir.path().join(format!("parallel.{format}"));
            let serial = ReadStatWriter::new(
                WriteConfig::new(format).output(&serial_path).unwrap(),
                schema.clone(),
            )
            .unwrap();
            assert_eq!(serial.finish().unwrap(), 0);

            let parallel = ParallelTextWriter::new(
                WriteConfig::new(format).output(&parallel_path).unwrap(),
                schema.clone(),
            )
            .unwrap();
            assert_eq!(parallel.finish().unwrap(), 0);

            assert_eq!(
                std::fs::read(serial_path).unwrap(),
                std::fs::read(parallel_path).unwrap()
            );
        }
    }

    #[test]
    fn rejects_schema_mismatch() {
        let schema = Arc::new(Schema::empty());
        let mut writer = ReadStatWriter::new(WriteConfig::new(OutFormat::Csv), schema).unwrap();
        let other = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Utf8,
            true,
        )]));
        assert!(matches!(
            writer.write(&RecordBatch::new_empty(other)),
            Err(ReadStatError::SchemaMismatch)
        ));
    }

    #[test]
    fn empty_output_for_each_enabled_format() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::empty());
        let formats = [
            OutFormat::Csv,
            OutFormat::Feather,
            OutFormat::Ndjson,
            OutFormat::Parquet,
        ];
        for format in formats {
            let path = dir.path().join(format!("empty.{format}"));
            let config = WriteConfig::new(format).output(&path).unwrap();
            let writer = ReadStatWriter::new(config, schema.clone()).unwrap();
            assert_eq!(writer.finish().unwrap(), 0);
            assert!(path.exists());
        }
    }

    #[test]
    fn output_open_race_and_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("race.csv");
        let config = WriteConfig::new(OutFormat::Csv).output(&path).unwrap();
        std::fs::write(&path, "sentinel").unwrap();
        let schema = Arc::new(Schema::empty());
        let writer = ReadStatWriter::new(config, schema.clone()).unwrap();
        assert!(matches!(
            writer.finish(),
            Err(ReadStatError::OutputFileExists(_))
        ));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "sentinel");

        let config = WriteConfig::new(OutFormat::Csv)
            .output(&path)
            .unwrap()
            .overwrite(true);
        let writer = ReadStatWriter::new(config, schema).unwrap();
        writer.finish().unwrap();
        assert_ne!(std::fs::read_to_string(path).unwrap(), "sentinel");
    }

    #[test]
    fn drop_before_finish_preserves_destination_and_cleans_staging() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("preserve.csv");
        std::fs::write(&path, "sentinel").unwrap();
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let config = WriteConfig::new(OutFormat::Csv)
            .output(&path)
            .unwrap()
            .overwrite(true);
        let mut writer = ReadStatWriter::new(config, schema.clone()).unwrap();
        writer.write(&test_batch(schema, &["new"])).unwrap();
        drop(writer);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "sentinel");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn no_overwrite_destination_raced_before_publication() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raced.csv");
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let config = WriteConfig::new(OutFormat::Csv).output(&path).unwrap();
        let mut writer = ReadStatWriter::new(config, schema.clone()).unwrap();
        writer.write(&test_batch(schema, &["new"])).unwrap();
        std::fs::write(&path, "racer").unwrap();
        assert!(matches!(
            writer.finish(),
            Err(ReadStatError::OutputFileExists(_))
        ));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "racer");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
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
