//! SQL query execution via Apache DataFusion.
//!
//! Registers Arrow [`RecordBatch`] data as an in-memory table in a DataFusion
//! [`SessionContext`], executes a SQL query, and returns the results as a
//! `Vec<RecordBatch>`.

use arrow_array::RecordBatch;
use arrow_csv::WriterBuilder as CsvWriterBuilder;
use arrow_ipc::writer::FileWriter as IpcFileWriter;
use arrow_json::LineDelimitedWriter as JsonLineDelimitedWriter;
use arrow_schema::SchemaRef;
use datafusion::datasource::MemTable;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures::StreamExt;
use parquet::{
    arrow::ArrowWriter as ParquetArrowWriter,
    basic::{BrotliLevel, Compression as ParquetCompressionCodec, GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use std::io::BufWriter;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::err::ReadStatError;
use crate::rs_data::ReadStatData;
use crate::rs_path::ReadStatPath;
use crate::rs_write_config::{OutFormat, ParquetCompression};

/// Executes a SQL query against in-memory Arrow data.
///
/// Registers the provided batches as a table named `table_name` in a
/// DataFusion [`SessionContext`], runs the SQL query, and collects the
/// results.
///
/// # Arguments
///
/// * `batches` - The data to query, as one or more [`RecordBatch`]es
/// * `schema` - The Arrow schema shared by all batches
/// * `table_name` - The name used to reference the table in SQL
/// * `sql` - The SQL query string to execute
pub fn execute_sql(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, ReadStatError> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(execute_sql_async(batches, schema, table_name, sql))
}

async fn execute_sql_async(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, ReadStatError> {
    let ctx = SessionContext::new();

    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(table_name, Arc::new(table))?;

    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;

    Ok(results)
}

/// A [`PartitionStream`] implementation that reads `RecordBatch`es from a
/// crossbeam channel, allowing DataFusion to consume data as it arrives
/// without collecting everything into memory first.
#[derive(Debug)]
struct ChannelPartitionStream {
    schema: SchemaRef,
    receiver: Arc<Mutex<Option<crossbeam::channel::Receiver<(ReadStatData, ReadStatPath, usize)>>>>,
}

impl ChannelPartitionStream {
    fn new(
        schema: SchemaRef,
        receiver: crossbeam::channel::Receiver<(ReadStatData, ReadStatPath, usize)>,
    ) -> Self {
        Self {
            schema,
            receiver: Arc::new(Mutex::new(Some(receiver))),
        }
    }
}

impl PartitionStream for ChannelPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<datafusion::execution::TaskContext>,
    ) -> SendableRecordBatchStream {
        let receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .expect("ChannelPartitionStream::execute called more than once");

        let stream = futures::stream::iter(
            receiver
                .into_iter()
                .filter_map(|(d, _, _)| d.batch)
                .map(Ok),
        );

        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// Executes a SQL query by streaming data from a crossbeam channel through
/// DataFusion, avoiding double-materialization of the full dataset.
///
/// The receiver is consumed directly by DataFusion's query engine via
/// [`StreamingTable`], and results are collected via `execute_stream()`.
pub fn execute_sql_stream(
    receiver: crossbeam::channel::Receiver<(ReadStatData, ReadStatPath, usize)>,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, ReadStatError> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(execute_sql_stream_async(receiver, schema, table_name, sql))
}

async fn execute_sql_stream_async(
    receiver: crossbeam::channel::Receiver<(ReadStatData, ReadStatPath, usize)>,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, ReadStatError> {
    let ctx = SessionContext::new();

    let partition = ChannelPartitionStream::new(schema.clone(), receiver);
    let table = StreamingTable::try_new(schema, vec![Arc::new(partition)])?;
    ctx.register_table(table_name, Arc::new(table))?;

    let df = ctx.sql(sql).await?;
    let mut stream = df.execute_stream().await?;

    let mut results = Vec::new();
    while let Some(batch) = stream.next().await {
        results.push(batch?);
    }

    Ok(results)
}

/// Executes a SQL query by streaming data from a crossbeam channel and writes
/// the results directly to an output file, avoiding intermediate collection.
///
/// This combines [`execute_sql_stream`] and [`write_sql_results`] into one
/// streaming pass for the Data command path.
pub fn execute_sql_and_write_stream(
    receiver: crossbeam::channel::Receiver<(ReadStatData, ReadStatPath, usize)>,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
    output_path: &Path,
    format: OutFormat,
    compression: Option<ParquetCompression>,
    compression_level: Option<u32>,
) -> Result<(), ReadStatError> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(execute_sql_and_write_stream_async(
        receiver,
        schema,
        table_name,
        sql,
        output_path,
        format,
        compression,
        compression_level,
    ))
}

async fn execute_sql_and_write_stream_async(
    receiver: crossbeam::channel::Receiver<(ReadStatData, ReadStatPath, usize)>,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
    output_path: &Path,
    format: OutFormat,
    compression: Option<ParquetCompression>,
    compression_level: Option<u32>,
) -> Result<(), ReadStatError> {
    let ctx = SessionContext::new();

    let partition = ChannelPartitionStream::new(schema.clone(), receiver);
    let table = StreamingTable::try_new(schema, vec![Arc::new(partition)])?;
    ctx.register_table(table_name, Arc::new(table))?;

    let df = ctx.sql(sql).await?;
    let mut stream = df.execute_stream().await?;

    // Collect all result batches — we need the output schema (which may differ
    // from the input schema due to projections/aggregations) before we can open
    // a writer, and some formats (Feather/IPC) need all data before finishing.
    let mut result_batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = stream.next().await {
        result_batches.push(batch?);
    }

    write_sql_results(&result_batches, output_path, format, compression, compression_level)?;

    Ok(())
}

/// Writes SQL result batches to an output file in the specified format.
pub fn write_sql_results(
    batches: &[RecordBatch],
    output_path: &Path,
    format: OutFormat,
    compression: Option<ParquetCompression>,
    compression_level: Option<u32>,
) -> Result<(), ReadStatError> {
    if batches.is_empty() {
        return Ok(());
    }
    let schema = batches[0].schema();

    match format {
        OutFormat::csv => {
            let f = std::fs::File::create(output_path)?;
            let mut writer = CsvWriterBuilder::new()
                .with_header(true)
                .build(BufWriter::new(f));
            for batch in batches {
                writer.write(batch)?;
            }
        }
        OutFormat::feather => {
            let f = std::fs::File::create(output_path)?;
            let mut writer = IpcFileWriter::try_new(BufWriter::new(f), &schema)?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        OutFormat::ndjson => {
            let f = std::fs::File::create(output_path)?;
            let mut writer = JsonLineDelimitedWriter::new(BufWriter::new(f));
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        OutFormat::parquet => {
            let f = std::fs::File::create(output_path)?;
            let codec = resolve_parquet_compression(compression, compression_level)?;
            let props = WriterProperties::builder()
                .set_compression(codec)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
                .build();
            let mut writer = ParquetArrowWriter::try_new(
                BufWriter::new(f),
                schema,
                Some(props),
            )?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.close()?;
        }
    }
    Ok(())
}

fn resolve_parquet_compression(
    compression: Option<ParquetCompression>,
    compression_level: Option<u32>,
) -> Result<ParquetCompressionCodec, ReadStatError> {
    let codec = match compression {
        Some(ParquetCompression::Uncompressed) => ParquetCompressionCodec::UNCOMPRESSED,
        Some(ParquetCompression::Snappy) => ParquetCompressionCodec::SNAPPY,
        Some(ParquetCompression::Gzip) => {
            if let Some(level) = compression_level {
                let gzip_level = GzipLevel::try_new(level)
                    .map_err(|e| ReadStatError::Other(format!("Invalid Gzip compression level: {}", e)))?;
                ParquetCompressionCodec::GZIP(gzip_level)
            } else {
                ParquetCompressionCodec::GZIP(GzipLevel::default())
            }
        }
        Some(ParquetCompression::Lz4Raw) => ParquetCompressionCodec::LZ4_RAW,
        Some(ParquetCompression::Brotli) => {
            if let Some(level) = compression_level {
                let brotli_level = BrotliLevel::try_new(level)
                    .map_err(|e| ReadStatError::Other(format!("Invalid Brotli compression level: {}", e)))?;
                ParquetCompressionCodec::BROTLI(brotli_level)
            } else {
                ParquetCompressionCodec::BROTLI(BrotliLevel::default())
            }
        }
        Some(ParquetCompression::Zstd) => {
            if let Some(level) = compression_level {
                let zstd_level = ZstdLevel::try_new(level as i32)
                    .map_err(|e| ReadStatError::Other(format!("Invalid Zstd compression level: {}", e)))?;
                ParquetCompressionCodec::ZSTD(zstd_level)
            } else {
                ParquetCompressionCodec::ZSTD(ZstdLevel::default())
            }
        }
        None => ParquetCompressionCodec::SNAPPY,
    };
    Ok(codec)
}

/// Reads a SQL query from a file path.
pub fn read_sql_file(path: &std::path::Path) -> Result<String, ReadStatError> {
    let sql = std::fs::read_to_string(path)?;
    let sql = sql.trim().to_string();
    if sql.is_empty() {
        return Err(ReadStatError::Other(
            "SQL file is empty".to_string(),
        ));
    }
    Ok(sql)
}
