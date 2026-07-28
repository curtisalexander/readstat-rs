[< Back to README](../README.md)

# Usage

> 💡 **Quick reference:** A one-page visual [**CLI Cheatsheet**](readstat-cheatsheet.html) is available for at-a-glance lookup of subcommands, flags, and common workflows.  This page is the full reference and goes deeper on memory, parallelism, and metadata round-trips.

After either [building](BUILDING.md) or [installing](../README.md#package-cli-install), the binary is invoked using [subcommands](https://docs.rs/clap/latest/clap/_derive/_tutorial/index.html#subcommands).  Currently, the following subcommands have been implemented:
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
- `convert` &rarr; converts to `csv`, `feather`, `ndjson`, or `parquet`

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

### Skipping the row count

Computing the row count requires traversing the entire file.  If only variable-level metadata is needed (names, types, labels, formats), pass `--skip-row-count` to short-circuit row enumeration:

```sh
readstat metadata /some/dir/to/example.sas7bdat --skip-row-count
```

In that mode the human-readable output reports the row count as `unknown` and JSON
uses `"row_count": null`. Parsing returns as soon as the header and variable
definitions have been read.

### Suppressing the progress bar

By default `metadata`, `preview`, and `convert` render a progress bar while the file is being parsed. Pass `--no-progress` to suppress it.

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

## Convert
`convert` infers output from `.csv`, `.feather`, `.ndjson`, or `.parquet`. An explicit `--format` must match the extension; unknown extensions and mismatches are errors. With no `--output`, CSV is written to stdout. Diagnostics are written to stderr.

The old `data` spelling remains a compatibility alias; new usage should use `convert`.

Supported formats:
- `csv`
- `feather`
- `ndjson`
- `parquet`

By default `convert` refuses to overwrite an existing output file. Pass `--overwrite` to replace it:

```sh
readstat convert /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --overwrite
```

### `csv`
To write parsed data (as `csv`) to a file, invoke the following (default is to write all parsed data to the specified file).

Omit `--output` to stream CSV to stdout (for example, `readstat convert example.sas7bdat | head`). With a `.csv` output path, the format is inferred:

```sh
readstat convert /some/dir/to/example.sas7bdat --output /some/dir/to/example.csv
```

To write the first 100 rows of parsed data (as `csv`) to a file, invoke the following.

```sh
readstat convert /some/dir/to/example.sas7bdat --output /some/dir/to/example.csv --rows 100
```

### `feather`
To write parsed data (as `feather`) to a file, invoke the following (default is to write all parsed data to the specified file).

```sh
readstat convert /some/dir/to/example.sas7bdat --output /some/dir/to/example.feather
```

To write the first 100 rows of parsed data (as `feather`) to a file, invoke the following.

```sh
readstat convert /some/dir/to/example.sas7bdat --output /some/dir/to/example.feather --rows 100
```

### `ndjson`
To write parsed data (as `ndjson`) to a file, invoke the following (default is to write all parsed data to the specified file).

```sh
readstat convert /some/dir/to/example.sas7bdat --output /some/dir/to/example.ndjson
```

To write the first 100 rows of parsed data (as `ndjson`) to a file, invoke the following.

```sh
readstat convert /some/dir/to/example.sas7bdat --output /some/dir/to/example.ndjson --rows 100
```

### `parquet`
To write parsed data (as `parquet`) to a file, invoke the following (default is to write all parsed data to the specified file).

```sh
readstat convert /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet
```

To write the first 100 rows of parsed data (as `parquet`) to a file, invoke the following.

```sh
readstat convert /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --rows 100
```

To write parsed data (as `parquet`) to a file with specific compression settings, invoke the following:

```sh
readstat convert /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --compression zstd --compression-level 3
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
readstat convert /some/dir/to/example.sas7bdat --output out.parquet --columns Brand,Model,EngineSize
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
readstat convert /some/dir/to/example.sas7bdat --output out.parquet --columns-file columns.txt
```

### Preview with column selection

```sh
readstat preview /some/dir/to/example.sas7bdat --columns Brand,Model,EngineSize
```

## Parallelism

The `convert` subcommand uses bounded parallel writing by default for CSV,
NDJSON, and Parquet files. The one-pass reader remains single-parser and feeds
ordered batches through a bounded channel, so write parallelism does not change
input row order.

- For CSV and NDJSON, groups of at most four input batches are encoded
  concurrently into independent byte buffers, then committed in order. CSV
  emits exactly one header.
- For Parquet, the columns of each row group are encoded concurrently, then
  appended to one output file in schema order without decoding or re-encoding.

CSV stdout remains sequential because transactional ordered assembly requires
file output. Feather remains sequential: its writer is already hidden behind
parsing in the canonical benchmark and Arrow IPC has no public zero-reencode
file assembly API comparable to Parquet column-chunk append. SQL output also
remains sequential.

For unusually wide or string-heavy datasets, disable parallel encoding to
reduce peak memory:
```sh
readstat convert /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --serial-write
```

## Memory Considerations

### Bounded Reader/Writer Pipeline

One ReadStat parser emits batches into a bounded channel
(capacity 10) while the writer consumes them. At most 10 queued batches plus
the active reader and writer batches are held, providing backpressure when the
writer is slower. For very wide, string-heavy datasets, lower `--stream-rows`
to reduce each batch's memory footprint.

```
Bounded Conversion Pipeline
===========================

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

### Default Parallel Writes

The channel remains bounded. Parallel CSV/NDJSON retains at most four input
batches and their encoded byte buffers in addition to the channel; lower
`--stream-rows` for wide rows. Parallel Parquet retains at most one incomplete
row group in addition to queued input batches. Arrow slices share their source
buffers, and a full row group's leaf columns are encoded concurrently. Encoded
chunks are committed in deterministic schema order before the next row group.
These are row-count bounds, not strict byte bounds: unusually wide or
string-heavy rows can still consume substantial memory.

```
Default Parallel Parquet Write
==============================

 Reader ──> bounded channel ──> row-group accumulator
                                      |
                         parallel column encoding
                         /         |          \
                      column 0  column 1  ... column N
                         \         |          /
                          ordered row-group commit
                                      |
                               final Parquet file
```

### SQL Queries (`--sql` / `--sql-file`)

SQL is an opt-in CLI feature because its DataFusion query engine substantially increases the binary size. Official release binaries and a normal `cargo install readstat-cli` omit it. Install an SQL-enabled CLI with `cargo install readstat-cli --features sql`. The `readstat` library continues to enable SQL by default and offers synchronous and asynchronous buffered and streaming APIs. Buffered batches support repeated scans; only channel-backed streaming input is single-execution because execution consumes its receiver.

Provide the query inline with `--sql "SELECT ..."`, or point at a file containing the query with `--sql-file path/to/query.sql`. The table name is the input file stem (e.g. `cars` for `cars.sas7bdat`). `--sql` and `--sql-file` are mutually exclusive with each other and with `--columns`/`--columns-file`.

```sh
# inline query
readstat convert cars.sas7bdat --output out.parquet --sql "SELECT make, mpg FROM cars WHERE mpg > 30"

# query from a file
readstat convert cars.sas7bdat --output out.parquet --sql-file query.sql
```

SQL queries require the full dataset to be materialized in memory via DataFusion's `MemTable` before query execution.  For large files this may result in significant memory usage.  Queries that filter rows (e.g. `SELECT ... WHERE ...`) will reduce the _output_ size but the _input_ must still be fully loaded.

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

## Reading Metadata from Output Files

When converting to Parquet or Feather, readstat-rs preserves column metadata (labels, SAS format strings, and storage widths) as Arrow field metadata. Schema-level metadata includes the table label when present.

The following metadata keys may appear on each field:

| Key | Description | Condition |
|-----|-------------|-----------|
| `label` | User-assigned variable label | Non-empty |
| `sas_format` | SAS format string (e.g. `DATE9`, `BEST12`, `$30`) | Non-empty |
| `storage_width` | Number of bytes used to store the variable | Always |
| `display_width` | Display width hint from the file | Non-zero |

Schema-level metadata:

| Key | Description | Condition |
|-----|-------------|-----------|
| `table_label` | User-assigned file label | Non-empty |

### Reading metadata with Python (pyarrow)

```python
import pyarrow.parquet as pq

schema = pq.read_schema("example.parquet")

# Table-level metadata
print(schema.metadata.get(b"table_label", b"").decode())

# Per-column metadata
for field in schema:
    meta = field.metadata or {}
    print(f"{field.name}:")
    print(f"  label:         {meta.get(b'label', b'').decode()}")
    print(f"  sas_format:    {meta.get(b'sas_format', b'').decode()}")
    print(f"  storage_width: {meta.get(b'storage_width', b'').decode()}")
    print(f"  display_width: {meta.get(b'display_width', b'').decode()}")
```

### Reading metadata with R (arrow)

```r
library(arrow)

schema <- read_parquet("example.parquet", as_data_frame = FALSE)$schema

# Per-column metadata
for (field in schema) {
  cat(field$name, "\n")
  cat("  label:        ", field$metadata$label, "\n")
  cat("  sas_format:   ", field$metadata$sas_format, "\n")
  cat("  storage_width:", field$metadata$storage_width, "\n")
  cat("  display_width:", field$metadata$display_width, "\n")
}
```

## Reader
The `preview` and `convert` subcommands include a parameter for `--reader`.  The possible values for `--reader` include the following.
- `mem` &rarr; Parse and read the entire `sas7bdat` into memory before writing to either standard out or a file
- `stream` (default) &rarr; Parse and read at most `stream-rows` into memory before writing to disk
    - `stream-rows` may be set via the command line parameter `--stream-rows` or if elided will default to 10,000 rows

**Why is this useful?**
- `mem` is useful for testing purposes
- `stream` is useful for keeping memory usage low for large datasets (and hence is the default)
- In general, users should not need to deviate from the default &mdash; `stream` &mdash; unless they have a specific need
- In addition, by enabling these options as command line parameters [hyperfine](BENCHMARKING.md#benchmarking-with-hyperfine) may be used to benchmark across an assortment of file sizes

## Debug
Debug information is printed to standard error by setting the environment variable `RUST_LOG=debug` before the call to `readstat`.

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
readstat convert --help
```
