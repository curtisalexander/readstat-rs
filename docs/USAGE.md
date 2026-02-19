[< Back to README](../README.md)

# Usage

After either [building](BUILDING.md) or [installing](../README.md#install), the binary is invoked using [subcommands](https://docs.rs/clap/latest/clap/_derive/_tutorial/index.html#subcommands).  Currently, the following subcommands have been implemented:
- `metadata` &rarr; writes the following to standard out or json
    - row count
    - variable count
    - table name
    - table label
    - file encoding
    - format version
    - bitness
    - creation time
    - modified time
    - compression
    - byte order
    - variable names
    - variable type classes
    - variable types
    - variable labels
    - variable format classes
    - variable formats
    - arrow data types
- `preview` &rarr; writes the first 10 rows (or optionally the number of rows provided by the user) of parsed data in `csv` format to standard out
- `data` &rarr; writes parsed data in `csv`, `feather`, `ndjson`, or `parquet` format to a file

## Metadata
To write metadata to standard out, invoke the following.

```sh
readstat metadata /some/dir/to/example.sas7bdat
```

To write metadata to json, invoke the following.  This is useful for reading the metadata programmatically.

```sh
readstat metadata /some/dir/to/example.sas7bdat --as-json
```

The JSON output contains file-level metadata and a `vars` object keyed by variable index.  This makes it straightforward to search for a particular column by piping the output to [`jq`](https://jqlang.github.io/jq/) or Python.

### Search for a column with `jq`

```sh
# Find the variable entry whose var_name matches "Make"
readstat metadata /some/dir/to/example.sas7bdat --as-json \
  | jq '.vars | to_entries[] | select(.value.var_name == "Make") | .value'
```

### Search for a column with Python

```sh
# Find the variable entry whose var_name matches "Make"
readstat metadata /some/dir/to/example.sas7bdat --as-json \
  | python -c "
import json, sys
md = json.load(sys.stdin)
match = [v for v in md['vars'].values() if v['var_name'] == 'Make']
if match:
    print(json.dumps(match[0], indent=2))
"
```

## Preview Data
To write parsed data (as a `csv`) to standard out, invoke the following (default is to write the first 10 rows).

```sh
readstat preview /some/dir/to/example.sas7bdat
```

To write the first 100 rows of parsed data (as a `csv`) to standard out, invoke the following.

```sh
readstat preview /some/dir/to/example.sas7bdat --rows 100
```

## Data
:memo: The `data` subcommand includes a parameter for `--format`, which is the file format that is to be written.  Currently, the following formats have been implemented:
- `csv`
- `feather`
- `ndjson`
- `parquet`

### `csv`
To write parsed data (as `csv`) to a file, invoke the following (default is to write all parsed data to the specified file).

The default `--format` is `csv`.  Thus, the parameter is elided from the below examples.

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.csv
```

To write the first 100 rows of parsed data (as `csv`) to a file, invoke the following.

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.csv --rows 100
```

### `feather`
To write parsed data (as `feather`) to a file, invoke the following (default is to write all parsed data to the specified file).

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.feather --format feather
```

To write the first 100 rows of parsed data (as `feather`) to a file, invoke the following.

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.feather --format feather --rows 100
```

### `ndjson`
To write parsed data (as `ndjson`) to a file, invoke the following (default is to write all parsed data to the specified file).

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.ndjson --format ndjson
```

To write the first 100 rows of parsed data (as `ndjson`) to a file, invoke the following.

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.ndjson --format ndjson --rows 100
```

### `parquet`
To write parsed data (as `parquet`) to a file, invoke the following (default is to write all parsed data to the specified file).

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --format parquet
```

To write the first 100 rows of parsed data (as `parquet`) to a file, invoke the following.

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --format parquet --rows 100
```

To write parsed data (as `parquet`) to a file with specific compression settings, invoke the following:

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --format parquet --compression zstd --compression-level 3
```

## Column Selection

Select specific columns to include when converting or previewing data.

### Step 1: View available columns

```sh
readstat metadata /some/dir/to/example.sas7bdat
```

Or as JSON for programmatic use with `jq`:

```sh
readstat metadata /some/dir/to/example.sas7bdat --as-json \
  | jq '.vars | to_entries[] | .value.var_name'
```

Or with Python:

```sh
readstat metadata /some/dir/to/example.sas7bdat --as-json \
  | python -c "
import json, sys
md = json.load(sys.stdin)
for v in md['vars'].values():
    print(v['var_name'])
"
```

### Step 2: Select columns on the command line

```sh
readstat data /some/dir/to/example.sas7bdat --output out.parquet --format parquet --columns Brand,Model,EngineSize
```

### Step 2 (alt): Select columns from a file

Create `columns.txt`:
```
# Columns to extract from the dataset
Brand
Model
EngineSize
```

Then pass it to the CLI:
```sh
readstat data /some/dir/to/example.sas7bdat --output out.parquet --format parquet --columns-file columns.txt
```

### Preview with column selection

```sh
readstat preview /some/dir/to/example.sas7bdat --columns Brand,Model,EngineSize
```

## Parallelism
The `data` subcommand includes parameters for both _**parallel reading**_ and _**parallel writing**_:

### Parallel Reading (`--parallel`)
If invoked, the _**reading**_ of a `sas7bdat` will occur in parallel.  If the total rows to process is greater than `stream-rows` (if unset, the default rows to stream is 10,000), then each chunk of rows is read in parallel.  Note that all processors on the user's machine are used with the `--parallel` option.  In the future, may consider allowing the user to throttle this number.

:heavy_exclamation_mark: Utilizing the `--parallel` parameter will increase memory usage &mdash; all chunks are read in parallel and collected in memory before being sent to the writer.  In addition, because all processors are utilized, CPU usage may be maxed out during reading.  Row ordering from the original `sas7bdat` is preserved.

### Parallel Writing (`--parallel-write`)
When combined with `--parallel`, the `--parallel-write` flag enables _**parallel writing**_ for Parquet format files. This can significantly improve write performance for large datasets by:
- Writing record batches to temporary files in parallel using all available processors
- Merging the temporary files into the final output
- Using spooled temporary files that keep data in memory until a threshold is reached

**Note:** Parallel writing currently only supports the Parquet format. Other formats (CSV, Feather, NDJSON) will use optimized sequential writes with BufWriter.

Example usage:
```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --format parquet --parallel --parallel-write
```

### Memory Buffer Size (`--parallel-write-buffer-mb`)
Controls the memory buffer size (in MB) before spilling to disk during parallel writes. Defaults to 100 MB. Valid range: 1-10240 MB.

Smaller buffers will cause data to spill to disk sooner, while larger buffers keep more data in memory. Choose based on your available memory and dataset size:
- **Small datasets (< 100 MB)**: Use default or larger buffer to keep everything in memory
- **Large datasets (> 1 GB)**: Consider smaller buffer (10-50 MB) to manage memory usage
- **Memory-constrained systems**: Use smaller buffer (1-10 MB)

Example with custom buffer size:
```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --format parquet --parallel --parallel-write --parallel-write-buffer-mb 200
```

:heavy_exclamation_mark: Parallel writing may write batches out of order. This is acceptable for Parquet files as the row order is preserved when merged.

## Memory Considerations

### Default: Sequential Writes

In the default sequential write mode, a bounded channel (capacity 10) connects the reader thread to the writer.  This means at most 10 chunks (each containing up to `stream-rows` rows) are held in memory at any time, providing natural backpressure when the writer is slower than the reader.  For most workloads this keeps memory usage reasonable, but for very wide datasets (hundreds of columns, string-heavy) each chunk can be large &mdash; consider lowering `--stream-rows` if memory is a concern.

```
Sequential Write (default)
==========================

 Reader Thread                 Bounded Channel (cap 10)            Main Thread
+---------------------+       +------------------------+       +---------------------+
|                     |       |                        |       |                     |
| +-----------+       | send  | +--+--+--+--+--+--+   | recv  | +-------+           |
| | chunk  1  |-------|------>| |  |  |  |  |  |  |   |------>| | write |---> file   |
| +-----------+       |       | +--+--+--+--+--+--+   |       | +-------+           |
| +-----------+       | send  |    channel is full!    |       |                     |
| | chunk  2  |-------|------>| +--+--+--+--+--+--+--+|       | +-------+           |
| +-----------+       |       | |  |  |  |  |  |  |  ||       | | write |---> file   |
| +-----------+       |       | +--+--+--+--+--+--+--+|       | +-------+           |
| | chunk  3  |-------|-XXXXX |                        |       |                     |
| +-----------+       | BLOCK | writer drains a slot   |       | +-------+           |
|   ... waits ...     |       |    +--+--+--+--+--+--+ |       | | write |---> file   |
| | chunk  3  |-------|------>|    |  |  |  |  |  |  | |       | +-------+           |
| +-----------+       | ok!   |    +--+--+--+--+--+--+ |       |                     |
|                     |       |                        |       |                     |
+---------------------+       +------------------------+       +---------------------+

 Memory at any moment: <= 10 chunks in the channel + 1 being written
 Backpressure: reader blocks when channel is full
```

### Parallel Writes (`--parallel-write`)

:memo: **`--parallel-write`**: Uses bounded-batch processing &mdash; batches are pulled from the channel in groups (up to 10 at a time), written in parallel to temporary Parquet files, then the next group is pulled.  This preserves the channel's backpressure so that memory usage stays bounded rather than loading the entire dataset at once.  All temporary files are merged into the final output at the end.

```
Parallel Write (--parallel --parallel-write)
============================================

 Reader Thread              Bounded Channel (cap 10)              Main Thread
+------------------+       +------------------------+       +-------------------------+
|                  |       |                        |       |                         |
| +----------+     | send  |                        | recv  |  Pull <= 10 batches     |
| | chunk  1 |-----|------>|  +-+-+-+-+-+-+-+-+-+-+ |------>|  +----+----+----+----+  |
| +----------+     |       |  | | | | | | | | | | | |       |  | b1 | b2 | .. | bN |  |
| +----------+     | send  |  +-+-+-+-+-+-+-+-+-+-+ |       |  +----+----+----+----+  |
| | chunk  2 |-----|------>|                        |       |    |    |         |      |
| +----------+     |       +------------------------+       |    v    v         v      |
| +----------+     |                                        |  Write in parallel      |
| | chunk  3 |-----|----> ...                               |  to temp .parquet files |
| +----------+     |                                        |    |    |         |      |
|     ...          |                                        |    v    v         v      |
|                  |                                        |  tmp_0 tmp_1 ... tmp_N   |
|                  |       +------------------------+       |                         |
| +----------+     | send  |                        | recv  |  Pull next <= 10        |
| | chunk 11 |-----|------>|  +-+-+-+-+-+-+-+-+-+-+ |------>|  +----+----+----+----+  |
| +----------+     |       |  | | | | | | | | | | | |       |  |b11 |b12 | .. | bM |  |
| +----------+     | send  |  +-+-+-+-+-+-+-+-+-+-+ |       |  +----+----+----+----+  |
| | chunk 12 |-----|------>|                        |       |    |    |         |      |
| +----------+     |       +------------------------+       |    v    v         v      |
|     ...          |                                        |  tmp_N+1  ...  tmp_M     |
+------------------+                                        |                         |
                                                            |  ... repeat until done  |
                                                            +-------------------------+
                                                                       |
                              +----------------------------------------+
                              |
                              v
                    +-------------------+       +--------------------+
                    |   Merge all temp  |       |                    |
                    |   .parquet files  |------>|  final output.pqt  |
                    |   in order        |       |                    |
                    +-------------------+       +--------------------+

 Memory at any moment: <= 10 chunks in channel + 10 being written
 Backpressure: preserved -- reader blocks while a batch group is being written
```

### SQL Queries (`--sql`)

:warning: **`--sql`** (feature-gated): SQL queries require the full dataset to be materialized in memory via DataFusion's `MemTable` before query execution.  For large files this may result in significant memory usage.  Queries that filter rows (e.g. `SELECT ... WHERE ...`) will reduce the _output_ size but the _input_ must still be fully loaded.

```
SQL Query Mode (--sql "SELECT ...")
===================================

 Reader Thread              Bounded Channel              Main Thread
+------------------+       +---------------+       +---------------------------+
|                  |       |               |       |                           |
| +----------+     | send  |               | recv  |  Collect ALL batches      |
| | chunk  1 |-----|------>|               |------>|  into memory (required    |
| +----------+     |       |               |       |  by DataFusion MemTable)  |
| +----------+     | send  |               |       |                           |
| | chunk  2 |-----|------>|               |------>|  +-----+-----+-----+     |
| +----------+     |       |               |       |  |  b1 |  b2 | ... |     |
|     ...          |       |               |       |  +-----+-----+-----+     |
| +----------+     | send  |               |       |         |                 |
| | chunk  N |-----|------>|               |------>|         v                 |
| +----------+     |       |               |       |  +-------------+         |
+------------------+       +---------------+       |  |  DataFusion |         |
                                                   |  |  SQL Engine |         |
                                                   |  +-------------+         |
                                                   |         |                 |
                                                   |         v                 |
                                                   |  Write filtered results  |
                                                   |  to output file          |
                                                   +---------------------------+

 Memory at peak: ALL chunks in memory (no backpressure)
 This is inherent to SQL execution over in-memory tables.
```

## Reader
The `preview` and `data` subcommands include a parameter for `--reader`.  The possible values for `--reader` include the following.
- `mem` &rarr; Parse and read the entire `sas7bdat` into memory before writing to either standard out or a file
- `stream` (default) &rarr; Parse and read at most `stream-rows` into memory before writing to disk
    - `stream-rows` may be set via the command line parameter `--stream-rows` or if elided will default to 10,000 rows

**Why is this useful?**
- `mem` is useful for testing purposes
- `stream` is useful for keeping memory usage low for large datasets (and hence is the default)
- In general, users should not need to deviate from the default &mdash; `stream` &mdash; unless they have a specific need
- In addition, by enabling these options as command line parameters [hyperfine](BENCHMARKING.md#benchmarking-with-hyperfine) may be used to benchmark across an assortment of file sizes

## Debug
Debug information is printed to standard out by setting the environment variable `RUST_LOG=debug` before the call to `readstat`.

:warning: This is quite verbose!  If using the [preview](#preview-data) or [data](#data) subcommand, will write debug information for _every single value_!

```sh
# Linux and macOS
RUST_LOG=debug readstat ...
```

```powershell
# Windows PowerShell
$env:RUST_LOG="debug"; readstat ...
```

## Help
For full details run with `--help`.

```sh
readstat --help
readstat metadata --help
readstat preview --help
readstat data --help
```
