# readstat

Pure Rust library for parsing SAS binary files (`.sas7bdat`) into Apache Arrow RecordBatch format. Uses FFI bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library for parsing.

**Note:** While the underlying [`readstat-sys`](https://crates.io/crates/readstat-sys) crate exposes bindings for all formats supported by ReadStat (SAS, SPSS, Stata), this crate currently only implements parsing and conversion for **SAS `.sas7bdat` files**.

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

For the full architecture overview, see [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).
