# readstat

Rust library for parsing SAS binary files (`.sas7bdat`) into Apache Arrow `RecordBatch` format. Parsing is performed via FFI bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library; the resulting data is exposed through a safe, idiomatic Rust API.

> **Note:** The ReadStat C library supports SAS, SPSS, and Stata file formats. The [`readstat-sys`](https://crates.io/crates/readstat-sys) crate exposes the **full** ReadStat API &mdash; all 125 functions across all formats. However, this crate only implements parsing and conversion for **SAS `.sas7bdat` files**. SPSS and Stata support is a possible future addition, but is **not planned at this time** &mdash; if you need those formats today, the `readstat-sys` bindings already expose the complete SPSS (`.sav`, `.zsav`, `.por`) and Stata (`.dta`) C API to build on.

**Minimum Supported Rust Version (MSRV):** `1.88` (Rust edition 2024).

## Quick Start

Configure a reader from a path (or use `from_bytes` / `from_mmap`) and read an entire file into one Arrow `RecordBatch`:

```rust,no_run
fn main() -> Result<(), readstat::ReadStatError> {
    let reader = readstat::ReadStatReader::from_path("data.sas7bdat")?
        .rows(0, None)
        .columns(["Make", "Model"])
        .chunk_rows(10_000);
    let batch = reader.read()?;
    println!("{} rows x {} columns", batch.num_rows(), batch.num_columns());
    Ok(())
}
```

Or read just the file/variable metadata, without loading any rows:

```rust,no_run
fn main() -> Result<(), readstat::ReadStatError> {
    let reader = readstat::ReadStatReader::from_path("data.sas7bdat")?;
    let md = reader.metadata()?;
    println!("{} rows x {} columns", md.row_count, md.var_count);
    Ok(())
}
```

Use `chunks()` to collect chunks or `visit()` for bounded-memory processing. To write, construct a `WriteConfig` with `new(format)` or extension-inferred `from_output(path)`, create `ReadStatWriter::new(config, schema)`, call `write(&batch)` for each batch, then consume it with `finish()` to atomically publish the output and obtain the written row count. See the [crate documentation](https://docs.rs/readstat) for complete examples.

## Features

Output format writers are feature-gated (all enabled by default):

- `csv` — CSV output via `arrow-csv`
- `parquet` — Parquet output (Snappy, Zstd, Brotli, Gzip, Lz4 compression)
- `feather` — Arrow IPC / Feather format
- `ndjson` — Newline-delimited JSON
- `sql` — DataFusion SQL query support (enabled by default), with synchronous and asynchronous APIs

## Key Types

- `ReadStatReader` — Primary path, owned-bytes, and mmap reader; supports row/column selection, metadata, whole reads, chunks, and visitors
- `ReadStatMetadata` — File-level metadata (row/var counts, encoding, compression, schema)
- `ReadStatWriter` — Writes Arrow batches to the requested output format
- `WriteConfig` — Output configuration (path, format, compression)

Buffered SQL inputs may be executed repeatedly. Use `record_batch_channel` with synchronous APIs and `async_record_batch_channel` with async APIs for bounded, error-aware streaming input. Only channel-backed SQL input is single-execution because its receiver is consumed by the first scan; async output encoding runs off the executor with bounded backpressure.

For the full architecture overview, see [docs/ARCHITECTURE.md](https://github.com/curtisalexander/readstat-rs/blob/main/docs/ARCHITECTURE.md).
