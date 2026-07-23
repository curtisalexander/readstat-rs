# readstat-cli

Binary crate producing the `readstat` CLI tool for converting SAS binary files (`.sas7bdat`) to other formats.

> **Note:** The ReadStat C library supports SAS, SPSS, and Stata file formats. The [`readstat-sys`](https://crates.io/crates/readstat-sys) crate exposes the **full** ReadStat API &mdash; all 125 functions across all formats. However, this CLI only supports **SAS `.sas7bdat` files**. SPSS and Stata support is a possible future addition, but is **not planned at this time** &mdash; if you need those formats today, the `readstat-sys` bindings already expose the complete SPSS (`.sav`, `.zsav`, `.por`) and Stata (`.dta`) C API to build on.

## Subcommands

- `metadata` — Print file metadata (row/var counts, labels, encoding, format version, etc.)
- `preview` — Preview first N rows as CSV to stdout
- `convert` — Convert to CSV, Feather, NDJSON, or Parquet (inferred from the output extension)

## Key Features

- Column selection (`--columns`, `--columns-file`)
- Streaming reads with configurable chunk size (`--stream-rows`)
- Parallel reading (`--parallel`) and parallel Parquet writing (`--parallel-write`)
- SQL queries via DataFusion (`--sql`, enabled by default)
- Parquet compression settings (`--compression`, `--compression-level`)

With no `--output`, conversion writes CSV to stdout. Progress, logs, and other diagnostics go to stderr, so stdout can be piped safely. An explicit format must agree with `.csv`, `.feather`, `.ndjson`, or `.parquet`; unknown extensions and mismatches are errors.

## Documentation

- [**CLI Cheatsheet**](https://curtisalexander.github.io/readstat-rs/readstat-cheatsheet.html) &mdash; one-page printable overview of subcommands, flags, and common workflows
- [Full CLI reference (docs/USAGE.md)](https://github.com/curtisalexander/readstat-rs/blob/main/docs/USAGE.md) &mdash; complete documentation with memory diagrams and metadata round-trip examples
