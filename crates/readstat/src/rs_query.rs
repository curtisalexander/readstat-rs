//! SQL query execution via Apache DataFusion.

use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::*;
use futures::StreamExt;

use crate::{ReadStatError, ReadStatWriter, WriteConfig};

/// Error-aware Arrow batch receiver used by streaming SQL queries.
pub type RecordBatchReceiver = crossbeam::channel::Receiver<Result<RecordBatch, ReadStatError>>;
/// Sending half of a streaming SQL input channel.
pub type RecordBatchSender = crossbeam::channel::Sender<Result<RecordBatch, ReadStatError>>;
/// Async receiving half of a streaming SQL input channel.
pub type AsyncRecordBatchReceiver = tokio::sync::mpsc::Receiver<Result<RecordBatch, ReadStatError>>;
/// Async sending half of a streaming SQL input channel.
pub type AsyncRecordBatchSender = tokio::sync::mpsc::Sender<Result<RecordBatch, ReadStatError>>;

/// Creates a bounded input channel for streaming SQL queries.
///
/// A bounded channel applies backpressure to producers; capacity zero creates
/// a rendezvous channel.
#[must_use]
pub fn record_batch_channel(capacity: usize) -> (RecordBatchSender, RecordBatchReceiver) {
    crossbeam::channel::bounded(capacity)
}

/// Creates a bounded, executor-friendly input channel for async SQL queries.
///
/// # Errors
///
/// Returns an error when `capacity` is zero.
pub fn async_record_batch_channel(
    capacity: usize,
) -> Result<(AsyncRecordBatchSender, AsyncRecordBatchReceiver), ReadStatError> {
    if capacity == 0 {
        return Err(ReadStatError::Other(
            "async record batch channel capacity must be greater than zero".into(),
        ));
    }
    Ok(tokio::sync::mpsc::channel(capacity))
}

fn runtime() -> Result<tokio::runtime::Runtime, ReadStatError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(ReadStatError::SyncSqlInAsyncRuntime);
    }
    Ok(tokio::runtime::Runtime::new()?)
}

/// Synchronously executes SQL against in-memory Arrow batches.
pub fn execute_sql(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, ReadStatError> {
    runtime()?.block_on(execute_sql_async(batches, schema, table_name, sql))
}

/// Executes SQL asynchronously against in-memory Arrow batches.
pub async fn execute_sql_async(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, ReadStatError> {
    let ctx = SessionContext::new();
    ctx.register_table(
        table_name,
        Arc::new(MemTable::try_new(schema, vec![batches])?),
    )?;
    collect_with_empty_batch(ctx.sql(sql).await?).await
}

async fn collect_with_empty_batch(df: DataFrame) -> Result<Vec<RecordBatch>, ReadStatError> {
    let schema = Arc::new(df.schema().as_arrow().clone());
    let results = df.collect().await?;
    Ok(if results.is_empty() {
        vec![RecordBatch::new_empty(schema)]
    } else {
        results
    })
}

/// A channel-backed partition is single-execution because receiving consumes
/// its input. Plans that scan it more than once return an execution error.
#[derive(Debug)]
struct ChannelPartitionStream {
    schema: SchemaRef,
    receiver: Arc<Mutex<Option<InputReceiver>>>,
}

impl ChannelPartitionStream {
    fn new(schema: SchemaRef, receiver: InputReceiver) -> Self {
        Self {
            schema,
            receiver: Arc::new(Mutex::new(Some(receiver))),
        }
    }
}

#[derive(Debug)]
enum InputReceiver {
    Blocking(RecordBatchReceiver),
    Async(AsyncRecordBatchReceiver),
}

impl PartitionStream for ChannelPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<datafusion::execution::TaskContext>) -> SendableRecordBatchStream {
        let receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let schema = self.schema.clone();
        let stream = match receiver {
            Some(receiver) => {
                let receiver = match receiver {
                    InputReceiver::Async(receiver) => receiver,
                    InputReceiver::Blocking(receiver) => {
                        let (sender, receiver_async) = tokio::sync::mpsc::channel(2);
                        // Crossbeam receive is blocking. Bridge it from a dedicated
                        // thread so polling DataFusion never blocks a Tokio worker.
                        std::thread::spawn(move || {
                            loop {
                                if sender.is_closed() {
                                    break;
                                }
                                match receiver
                                    .recv_timeout(std::time::Duration::from_millis(100))
                                {
                                    Ok(result) => {
                                        if sender.blocking_send(result).is_err() {
                                            break;
                                        }
                                    }
                                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => {}
                                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
                                }
                            }
                        });
                        receiver_async
                    }
                };
                futures::stream::unfold(receiver, |mut receiver| async move {
                    receiver.recv().await.map(|batch| {
                        let batch = batch.map_err(|error| {
                            datafusion::error::DataFusionError::External(Box::new(error))
                        });
                        (batch, receiver)
                    })
                })
                .left_stream()
            }
            None => futures::stream::once(async {
                Err(datafusion::error::DataFusionError::Execution(
                    "channel-backed StreamingTable can only be executed once; use execute_sql for plans that scan input multiple times".into(),
                ))
            })
            .right_stream(),
        };
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

fn streaming_context(
    receiver: InputReceiver,
    schema: SchemaRef,
    table_name: &str,
) -> Result<SessionContext, ReadStatError> {
    let ctx = SessionContext::new();
    let partition = ChannelPartitionStream::new(schema.clone(), receiver);
    ctx.register_table(
        table_name,
        Arc::new(StreamingTable::try_new(schema, vec![Arc::new(partition)])?),
    )?;
    Ok(ctx)
}

/// Synchronously executes SQL from a single-use channel of Arrow batches.
///
/// Input is consumed incrementally; query results are collected in memory.
pub fn execute_sql_stream(
    receiver: RecordBatchReceiver,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, ReadStatError> {
    runtime()?.block_on(execute_sql_from_input_async(
        InputReceiver::Blocking(receiver),
        schema,
        table_name,
        sql,
    ))
}

/// Asynchronously executes SQL from a single-use channel of Arrow batches.
///
/// Input is consumed incrementally without blocking the async executor; query
/// results are collected in memory. Plans that scan the input more than once
/// are unsupported.
pub async fn execute_sql_stream_async(
    receiver: AsyncRecordBatchReceiver,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, ReadStatError> {
    execute_sql_from_input_async(InputReceiver::Async(receiver), schema, table_name, sql).await
}

async fn execute_sql_from_input_async(
    receiver: InputReceiver,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, ReadStatError> {
    let ctx = streaming_context(receiver, schema, table_name)?;
    collect_with_empty_batch(ctx.sql(sql).await?).await
}

/// Synchronously streams SQL output directly to a configured writer.
pub fn execute_sql_and_write_stream(
    receiver: RecordBatchReceiver,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
    config: &WriteConfig,
) -> Result<usize, ReadStatError> {
    runtime()?.block_on(execute_sql_and_write_from_input_async(
        InputReceiver::Blocking(receiver),
        schema,
        table_name,
        sql,
        config,
    ))
}

/// Asynchronously writes each SQL output batch as soon as DataFusion produces it.
/// Plans that scan the channel-backed table more than once are unsupported.
pub async fn execute_sql_and_write_stream_async(
    receiver: AsyncRecordBatchReceiver,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
    config: &WriteConfig,
) -> Result<usize, ReadStatError> {
    execute_sql_and_write_from_input_async(
        InputReceiver::Async(receiver),
        schema,
        table_name,
        sql,
        config,
    )
    .await
}

async fn execute_sql_and_write_from_input_async(
    receiver: InputReceiver,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
    config: &WriteConfig,
) -> Result<usize, ReadStatError> {
    let ctx = streaming_context(receiver, schema, table_name)?;
    let df = ctx.sql(sql).await?;
    let result_schema = Arc::new(df.schema().as_arrow().clone());
    let mut stream = df.execute_stream().await?;
    enum Message {
        Batch(RecordBatch),
        Finish,
    }
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let config = config.clone();
    let writer_task = tokio::task::spawn_blocking(move || {
        let mut writer = ReadStatWriter::new(config, result_schema)?;
        while let Some(message) = receiver.blocking_recv() {
            match message {
                Message::Batch(batch) => writer.write(&batch)?,
                Message::Finish => return writer.finish(),
            }
        }
        Err(ReadStatError::Other(
            "SQL output was cancelled before the writer finished".into(),
        ))
    });
    while let Some(batch) = stream.next().await {
        sender
            .send(Message::Batch(batch?))
            .await
            .map_err(|_| ReadStatError::Other("SQL writer stopped unexpectedly".into()))?;
    }
    sender
        .send(Message::Finish)
        .await
        .map_err(|_| ReadStatError::Other("SQL writer stopped unexpectedly".into()))?;
    drop(sender);
    writer_task
        .await
        .map_err(|error| ReadStatError::Other(format!("SQL writer task failed: {error}")))?
}

/// Reads and validates a SQL query file.
pub fn read_sql_file(path: &std::path::Path) -> Result<String, ReadStatError> {
    let sql = std::fs::read_to_string(path)?.trim().to_string();
    if sql.is_empty() {
        return Err(ReadStatError::EmptySqlFile(path.to_path_buf()));
    }
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    fn input() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();
        (schema, batch)
    }

    #[tokio::test]
    async fn async_query_and_sync_runtime_guard() {
        let (schema, batch) = input();
        let result = execute_sql_async(vec![batch.clone()], schema.clone(), "t", "select * from t")
            .await
            .unwrap();
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(matches!(
            execute_sql(vec![batch], schema, "t", "select * from t"),
            Err(ReadStatError::SyncSqlInAsyncRuntime)
        ));
    }

    #[tokio::test]
    async fn streaming_propagates_channel_errors_and_preserves_empty_schema() {
        let (schema, batch) = input();
        let (sender, receiver) = async_record_batch_channel(1).unwrap();
        sender
            .send(Err(ReadStatError::Other("source failed".into())))
            .await
            .unwrap();
        drop(sender);
        let error = execute_sql_stream_async(receiver, schema.clone(), "t", "select * from t")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("source failed"));

        let (sender, receiver) = async_record_batch_channel(1).unwrap();
        sender.send(Ok(batch)).await.unwrap();
        drop(sender);
        let result = execute_sql_stream_async(receiver, schema, "t", "select * from t where 1=0")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 0);
        assert_eq!(result[0].num_columns(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn streaming_does_not_block_current_thread_runtime() {
        let (schema, batch) = input();
        let (sender, receiver) = async_record_batch_channel(1).unwrap();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            sender.send(Ok(batch)).await.unwrap();
        });
        let result = execute_sql_stream_async(receiver, schema, "t", "select * from t")
            .await
            .unwrap();
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    }
}
