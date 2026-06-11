# readstat

Rust library for parsing SAS binary files (`.sas7bdat`) into Apache Arrow `RecordBatch` format. Parsing is performed via FFI bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library; the resulting data is exposed through a safe, idiomatic Rust API.

> **Note:** The ReadStat C library supports SAS, SPSS, and Stata file formats. The [`readstat-sys`](https://crates.io/crates/readstat-sys) crate exposes the **full** ReadStat API &mdash; all 125 functions across all formats. However, this crate currently only implements parsing and conversion for **SAS `.sas7bdat` files**. SPSS and Stata formats are not supported.

**Minimum Supported Rust Version (MSRV):** `1.88` (Rust edition 2024).

## Quick Start

Read an entire file into a single Arrow `RecordBatch`:

```rust,no_run
fn main() -> Result<(), readstat::ReadStatError> {
    let batch = readstat::read_to_batch("data.sas7bdat")?;
    println!("{} rows x {} columns", batch.num_rows(), batch.num_columns());
    Ok(())
}
```

Or read just the file/variable metadata, without loading any rows:

```rust,no_run
fn main() -> Result<(), readstat::ReadStatError> {
    let md = readstat::read_metadata("data.sas7bdat")?;
    println!("{} rows x {} columns", md.row_count, md.var_count);
    Ok(())
}
```

For streaming large files in chunks, parallel reads, and column filtering, use the `ReadStatPath` / `ReadStatMetadata` / `ReadStatData` types directly — see the [crate documentation](https://docs.rs/readstat).

## Features

Output format writers are feature-gated (all enabled by default):

- `csv` — CSV output via `arrow-csv`
- `parquet` — Parquet output (Snappy, Zstd, Brotli, Gzip, Lz4 compression)
- `feather` — Arrow IPC / Feather format
- `ndjson` — Newline-delimited JSON
- `sql` — DataFusion SQL query support (optional, not enabled by default)

## Key Types

- `ReadStatData` — Coordinates FFI parsing, accumulates values directly into typed Arrow builders
- `ReadStatMetadata` — File-level metadata (row/var counts, encoding, compression, schema)
- `ReadStatWriter` — Writes Arrow batches to the requested output format
- `ReadStatPath` — Validated input file path
- `WriteConfig` — Output configuration (path, format, compression)

For the full architecture overview, see [docs/ARCHITECTURE.md](https://github.com/curtisalexander/readstat-rs/blob/main/docs/ARCHITECTURE.md).
