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
use datafusion::prelude::*;
use parquet::{
    arrow::ArrowWriter as ParquetArrowWriter,
    basic::{BrotliLevel, Compression as ParquetCompressionCodec, GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use crate::err::ReadStatError;
use crate::{OutFormat, ParquetCompression};

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
