# Future Enhancement: Streaming SQL Execution with DataFusion

## Summary

Replace the current collect-all-into-memory SQL execution path with a streaming approach using DataFusion's `StreamTable` and `execute_stream()` APIs. This would allow SQL queries (especially `SELECT ... WHERE ...`) to process large files without materializing the entire dataset in memory.

## Current Implementation

**File:** `crates/readstat/src/rs_query.rs`

The current flow collects all `RecordBatch`es into a `Vec` before handing them to DataFusion:

```
Channel --> .collect() into Vec<RecordBatch> --> MemTable --> df.collect() --> write results
```

Two call sites in `crates/readstat/src/lib.rs`:
- ~Line 479: main data pipeline (non-threaded path)
- ~Line 688: threaded pipeline, after `read_batches` are collected from the channel receiver

Both paths drain the entire channel into memory before DataFusion sees any data.

### `execute_sql` (rs_query.rs, line 38)

```rust
pub fn execute_sql(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, ReadStatError> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(execute_sql_async(batches, schema, table_name, sql))
}
```

### `execute_sql_async` (rs_query.rs, line 47)

```rust
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
```

## Proposed Streaming Approach

### Architecture

```
Reader Thread --> Bounded Channel --> RecordBatchStreamAdapter --> StreamTable
                                                                      |
                                                                      v
                                                               DataFusion SQL
                                                                      |
                                                                      v
                                                          df.execute_stream()
                                                                      |
                                                                      v
                                                          Write each batch to file
```

### Key Changes

#### 1. Input Side: Replace `MemTable` with `StreamTable`

DataFusion provides `datafusion::datasource::streaming::StreamTable` which accepts a `SendableRecordBatchStream` instead of `Vec<RecordBatch>`.

To wire the crossbeam channel receiver as a stream:

```rust
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;

// Wrap the crossbeam receiver as a futures::Stream
let schema = Arc::new(schema);
let receiver_stream = stream::iter(
    receiver.iter().filter_map(|(d, _, _)| d.batch).map(Ok)
);
let batch_stream = RecordBatchStreamAdapter::new(schema.clone(), receiver_stream);
```

Then register it:

```rust
use datafusion::datasource::streaming::StreamTable;
use datafusion::datasource::streaming::PartitionStream;

// StreamTable expects Vec<Arc<dyn PartitionStream>>
// You'll need a wrapper struct implementing PartitionStream
ctx.register_table(table_name, Arc::new(stream_table))?;
```

#### 2. Output Side: Replace `df.collect()` with `df.execute_stream()`

```rust
let df = ctx.sql(sql).await?;
let mut stream = df.execute_stream().await?;

while let Some(batch) = stream.next().await {
    let batch = batch?;
    writer.write(&batch)?;
}
```

This yields `RecordBatch`es one at a time rather than collecting all results.

#### 3. Signature Change for `execute_sql`

The function signature would need to change from accepting/returning `Vec<RecordBatch>` to accepting a receiver and returning a stream (or accepting a writer callback):

```rust
// Option A: Accept a channel receiver, return a stream
pub fn execute_sql_stream(
    receiver: crossbeam::channel::Receiver<(ReadStatData, ReadStatPath, usize)>,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
) -> Result<impl Iterator<Item = Result<RecordBatch, ReadStatError>>, ReadStatError>

// Option B: Accept a channel receiver and a writer, process end-to-end
pub fn execute_sql_and_write(
    receiver: crossbeam::channel::Receiver<(ReadStatData, ReadStatPath, usize)>,
    schema: SchemaRef,
    table_name: &str,
    sql: &str,
    writer: &mut ReadStatWriter,
    rsp: &ReadStatPath,
) -> Result<(), ReadStatError>
```

### Important Caveats

#### Operations That Cannot Stream

Some SQL operations inherently require full materialization regardless of the input source:

| Operation | Can Stream? | Why |
|-----------|-------------|-----|
| `SELECT ... WHERE ...` | Yes | Filter applied per-batch |
| `SELECT col1, col2` (projection) | Yes | Column pruning per-batch |
| `LIMIT N` | Yes | Stop after N rows |
| `GROUP BY` | No | Must see all data to compute groups |
| `ORDER BY` | No | Must see all data to sort |
| `COUNT(*)`, `SUM()`, `AVG()` | No | Must see all data for aggregation |
| `JOIN` | Depends | Hash joins buffer one side |

For non-streamable operations, DataFusion will internally buffer within its execution engine. The benefit of `StreamTable` is still real — it avoids the *double* materialization (once in our `Vec<RecordBatch>`, once inside DataFusion's `MemTable`).

#### Tokio Runtime Considerations

The current implementation creates a one-shot `tokio::runtime::Runtime` per query. The streaming approach would need the runtime to stay alive for the duration of stream consumption. This is already the case with the current `block_on` pattern but worth keeping in mind.

#### PartitionStream Implementation

`StreamTable` requires implementing the `PartitionStream` trait:

```rust
pub trait PartitionStream: Send + Sync {
    fn schema(&self) -> &SchemaRef;
    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream;
}
```

You'll need a small wrapper struct that holds the schema and produces the `RecordBatchStreamAdapter` from the channel receiver.

### Implementation Steps

1. **Create a `PartitionStream` wrapper** in `rs_query.rs` that wraps a crossbeam receiver as a `SendableRecordBatchStream`
2. **Add `execute_sql_stream` function** (or modify existing) that uses `StreamTable` + `execute_stream()`
3. **Update call sites in `lib.rs`** to pass the channel receiver directly to the SQL execution path instead of collecting first
4. **Keep `execute_sql` with `MemTable`** as a fallback or for the `write_sql_results` path (which needs `Vec<RecordBatch>` for the current writer API)
5. **Update `write_sql_results`** to accept a stream rather than a `Vec<RecordBatch>` if doing end-to-end streaming
6. **Test** with large files and various SQL operations to verify memory improvement
7. **Update README** Memory Considerations section — the SQL diagram would change from "ALL chunks in memory" to showing streaming behavior

### Dependencies

- `datafusion::datasource::streaming::StreamTable` — available in DataFusion 44+ (check current version in `Cargo.toml`)
- `futures` crate for `Stream` trait and `stream::iter`
- Confirm `RecordBatchStreamAdapter` API in the version of DataFusion used

### References

- DataFusion `StreamTable`: https://docs.rs/datafusion/latest/datafusion/datasource/streaming/struct.StreamTable.html
- DataFusion `PartitionStream`: https://docs.rs/datafusion/latest/datafusion/datasource/streaming/trait.PartitionStream.html
- DataFusion `RecordBatchStreamAdapter`: https://docs.rs/datafusion/latest/datafusion/physical_plan/stream/struct.RecordBatchStreamAdapter.html
- `DataFrame::execute_stream()`: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
