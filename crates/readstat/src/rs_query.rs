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
use datafusion::catalog::streaming::StreamingTable;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::*;
use futures::StreamExt;
use parquet::{arrow::ArrowWriter as ParquetArrowWriter, file::properties::WriterProperties};
use std::io::BufWriter;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::err::ReadStatError;
use crate::rs_data::ReadStatData;
use crate::rs_path::ReadStatPath;
use crate::rs_write_config::{
    OutFormat, ParquetCompression, WriteConfig, resolve_parquet_compression,
};

/// Channel receiver type for streaming parsed data chunks between threads.
///
/// Each message contains the parsed [`ReadStatData`], the source [`ReadStatPath`],
/// and the chunk index. Construct the matching sender/receiver pair with the
/// re-exported [`crossbeam`](crate::crossbeam) channel functions.
pub type ChunkReceiver = crossbeam::channel::Receiver<(ReadStatData, ReadStatPath, usize)>;

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
///
/// # Errors
///
/// Returns [`ReadStatError`] if the Tokio runtime cannot be created, the table
/// cannot be registered, or the query fails to plan or execute.
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
    let result_schema = Arc::new(df.schema().as_arrow().clone());
    let results = df.collect().await?;

    // A query may legitimately return zero rows (e.g. `WHERE 1=0`). Return a
    // single empty batch carrying the result schema so downstream writers can
    // still produce a valid (header-only) output file.
    if results.is_empty() {
        return Ok(vec![RecordBatch::new_empty(result_schema)]);
    }

    Ok(results)
}

/// A [`PartitionStream`] implementation that reads `RecordBatch`es from a
/// crossbeam channel, allowing DataFusion to consume data as it arrives
/// without collecting everything into memory first.
#[derive(Debug)]
struct ChannelPartitionStream {
    schema: SchemaRef,
    receiver: Arc<Mutex<Option<ChunkReceiver>>>,
}

impl ChannelPartitionStream {
    fn new(schema: SchemaRef, receiver: ChunkReceiver) -> Self {
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

    fn execute(&self, _ctx: Arc<datafusion::execution::TaskContext>) -> SendableRecordBatchStream {
        // The receiver is moved out on first execute, so this partition can be
        // consumed exactly once. Some query plans (notably self-joins) execute a
        // partition more than once; rather than panic inside the execution
        // operator — which would abort the whole process — surface a recoverable
        // query error so the caller gets an `Err` from the query instead.
        let Some(receiver) = self
            .receiver
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        else {
            let err = datafusion::error::DataFusionError::Execution(
                "ChannelPartitionStream can only be executed once; this query plan reads the \
                 streaming input more than once (e.g. a self-join). Re-run with the \
                 non-streaming SQL path (execute_sql), which buffers the data."
                    .to_string(),
            );
            let stream = futures::stream::once(async move { Err(err) });
            return Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream));
        };

        let stream =
            futures::stream::iter(receiver.into_iter().filter_map(|(d, _, _)| d.batch).map(Ok));

        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// Executes a SQL query by streaming data from a crossbeam channel through
/// DataFusion, avoiding double-materialization of the full dataset.
///
/// The receiver is consumed directly by DataFusion's query engine via
/// [`StreamingTable`], and results are collected via `execute_stream()`.
///
/// # Single-execution limit
///
/// The streaming input is consumed exactly once. Query plans that read the
/// table more than once (e.g. a self-join on the streamed table) will fail with
/// a DataFusion execution error. Use [`execute_sql`] (which buffers the data)
/// for such queries.
///
/// # Errors
///
/// Returns [`ReadStatError`] if the Tokio runtime cannot be created, the table
/// cannot be registered, or the query fails to plan or execute.
pub fn execute_sql_stream(
    receiver: ChunkReceiver,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, ReadStatError> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(execute_sql_stream_async(receiver, schema, table_name, sql))
}

async fn execute_sql_stream_async(
    receiver: ChunkReceiver,
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
///
/// The output path in `write_config` must be `Some`; returns an error otherwise.
///
/// # Single-execution limit
///
/// As with [`execute_sql_stream`], the streaming input is consumed exactly once;
/// query plans that read the table more than once (e.g. a self-join) fail with a
/// DataFusion execution error. Use the non-streaming path for those.
///
/// # Errors
///
/// Returns [`ReadStatError`] if the output path is missing, the Tokio runtime
/// cannot be created, the query fails, or writing the results fails.
pub fn execute_sql_and_write_stream(
    receiver: ChunkReceiver,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
    write_config: &WriteConfig,
) -> Result<(), ReadStatError> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(execute_sql_and_write_stream_async(
        receiver,
        schema,
        table_name,
        sql,
        write_config,
    ))
}

async fn execute_sql_and_write_stream_async(
    receiver: ChunkReceiver,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
    write_config: &WriteConfig,
) -> Result<(), ReadStatError> {
    let output_path = write_config
        .out_path
        .as_deref()
        .ok_or_else(|| ReadStatError::Other("Output path is required for SQL write".into()))?;

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

    write_sql_results(
        &result_batches,
        output_path,
        write_config.format,
        write_config.compression,
        write_config.compression_level,
    )?;

    Ok(())
}

/// Writes SQL result batches to an output file in the specified format.
///
/// Returns `Ok(())` without writing anything if `batches` is empty.
///
/// # Errors
///
/// Returns [`ReadStatError`] if the output file cannot be created or a write
/// fails for the chosen format.
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
        OutFormat::Csv => {
            let f = std::fs::File::create(output_path)?;
            let mut writer = CsvWriterBuilder::new()
                .with_header(true)
                .build(BufWriter::new(f));
            for batch in batches {
                writer.write(batch)?;
            }
        }
        OutFormat::Feather => {
            let f = std::fs::File::create(output_path)?;
            let mut writer = IpcFileWriter::try_new(BufWriter::new(f), &schema)?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        OutFormat::Ndjson => {
            let f = std::fs::File::create(output_path)?;
            let mut writer = JsonLineDelimitedWriter::new(BufWriter::new(f));
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        OutFormat::Parquet => {
            let f = std::fs::File::create(output_path)?;
            let codec = resolve_parquet_compression(compression, compression_level)?;
            let props = WriterProperties::builder()
                .set_compression(codec)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
                .build();
            let mut writer = ParquetArrowWriter::try_new(BufWriter::new(f), schema, Some(props))?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.close()?;
        }
    }
    Ok(())
}

/// Reads a SQL query from a file path.
///
/// # Errors
///
/// Returns [`ReadStatError`] if the file cannot be read or contains only
/// whitespace.
pub fn read_sql_file(path: &std::path::Path) -> Result<String, ReadStatError> {
    let sql = std::fs::read_to_string(path)?;
    let sql = sql.trim().to_string();
    if sql.is_empty() {
        return Err(ReadStatError::EmptySqlFile(path.to_path_buf()));
    }
    Ok(sql)
}
