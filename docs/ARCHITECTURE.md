# Architecture

Rust CLI tool and library that reads SAS binary files (`.sas7bdat`) and converts them to other formats (CSV, Feather, NDJSON, Parquet). Uses FFI bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library for parsing, and Apache Arrow for in-memory representation and output.

**Scope:** The `readstat-sys` crate exposes the full ReadStat C API, which supports SAS (`.sas7bdat`, `.xpt`), SPSS (`.sav`, `.zsav`, `.por`), and Stata (`.dta`). However, the `readstat`, `readstat-cli`, and `readstat-wasm` crates currently only implement parsing and conversion for **SAS `.sas7bdat` files**.

## Workspace Layout

```
readstat-rs/
├── Cargo.toml              # Workspace root (edition 2024, resolver 2)
├── crates/
│   ├── readstat/            # Library crate (parse SAS → Arrow, optional format writers)
│   ├── readstat-cli/        # Binary crate (CLI arg parsing, orchestration)
│   ├── readstat-sys/        # FFI bindings to ReadStat C library (bindgen)
│   ├── iconv-sys/           # FFI bindings to iconv (Windows only, package: readstat-iconv-sys)
│   ├── readstat-tests/      # Integration test suite
│   └── readstat-wasm/       # WebAssembly build (excluded from workspace)
└── docs/
```

## Crate Details

### `readstat` (v0.19.0) — Library Crate
**Path**: `crates/readstat/`

Pure library for parsing SAS binary files into Arrow RecordBatch format.
Output format writers (CSV, Feather, NDJSON, Parquet) are feature-gated.

Features: `csv`, `feather`, `ndjson`, `parquet` (all enabled by default), `sql`.

Key source modules in `crates/readstat/src/`:
| Module | Purpose |
|--------|---------|
| `lib.rs` | Public API exports |
| `cb.rs` | C callback functions for ReadStat (handle_metadata, handle_variable, handle_value) |
| `rs_data.rs` | Data reading, Arrow RecordBatch conversion |
| `rs_metadata.rs` | Metadata extraction, Arrow schema building |
| `rs_parser.rs` | ReadStatParser wrapper around C parser |
| `rs_path.rs` | Input path validation |
| `rs_write_config.rs` | Output configuration (path, format, compression) |
| `rs_var.rs` | Variable types and value handling |
| `rs_write.rs` | Output writers (CSV, Feather, NDJSON, Parquet) |
| `progress.rs` | `ProgressCallback` trait for parsing progress reporting |
| `rs_query.rs` | SQL query execution via DataFusion (feature-gated) |
| `formats.rs` | SAS format detection (118 date/time/datetime formats, regex-based) |
| `err.rs` | Error enum (41 variants mapping to C library errors) |
| `common.rs` | Utility functions |
| `rs_buffer_io.rs` | Buffer I/O operations |

Key public types:
- `ReadStatData` — coordinates FFI parsing, accumulates values directly into typed Arrow builders, produces Arrow RecordBatch
- `ReadStatMetadata` — file-level metadata (row/var counts, encoding, compression, schema)
- `ColumnBuilder` — enum wrapping 12 typed Arrow builders (StringBuilder, Float64Builder, Date32Builder, etc.); values are appended during FFI callbacks with zero intermediate allocation
- `ReadStatWriter` — writes output in requested format
- `ReadStatPath` — validated input file path
- `WriteConfig` — output configuration (path, format, compression)
- `OutFormat` — output format enum (Csv, Feather, Ndjson, Parquet)
- `ProgressCallback` — trait for receiving progress updates during parsing

Major dependencies: Arrow v57 ecosystem, Parquet (5 compression codecs, optional), Rayon, chrono, memmap2.

### `readstat-cli` (v0.19.0) — CLI Binary
**Path**: `crates/readstat-cli/`

Binary crate producing the `readstat` CLI tool. Uses clap with three subcommands:
- `metadata` — print file metadata (row/var counts, labels, encoding, etc.)
- `preview` — preview first N rows
- `data` — convert to output format (csv, feather, ndjson, parquet)

Owns CLI arg parsing, progress bars, colored output, and reader-writer thread orchestration.

Additional dependencies: clap v4, colored, indicatif, crossbeam, env_logger, path_abs.

### `readstat-sys` (v0.3.0) — FFI Bindings
**Path**: `crates/readstat-sys/`

`build.rs` compiles ~49 C source files from `vendor/ReadStat/` git submodule via the `cc` crate, then generates Rust bindings with `bindgen`. Exposes the **full** ReadStat API including support for SAS, SPSS, and Stata formats. Platform-specific linking for iconv and zlib:

| Platform | iconv | zlib | Notes |
|----------|-------|------|-------|
| **Windows** (`windows-msvc`) | Static — compiled from vendored `iconv-sys` submodule | Static — compiled via `libz-sys` crate | `iconv-sys` is a `cfg(windows)` dependency; needs `LIBCLANG_PATH` |
| **macOS** (`apple-darwin`) | Dynamic — system `libiconv` | `libz-sys` (uses system zlib) | iconv linked via `cargo:rustc-link-lib=iconv` |
| **Linux** (gnu/musl) | Dynamic — system library | `libz-sys` (prefers system, falls back to source) | No explicit iconv link directives; system linker resolves automatically |

Header include paths are propagated between crates using Cargo's `links` key:
- `readstat-iconv-sys` sets `cargo:include=...` which becomes `DEP_ICONV_INCLUDE` in `readstat-sys`
- `libz-sys` sets `cargo:include=...` which becomes `DEP_Z_INCLUDE` in `readstat-sys`

### `readstat-iconv-sys` (v0.3.0) — iconv FFI (Windows)
**Path**: `crates/iconv-sys/`

Windows-only (`#[cfg(windows)]`). Compiles libiconv from the `vendor/libiconv-win-build/` git submodule using the `cc` crate, producing a static library. On non-Windows platforms the build script is a no-op. The `links = "iconv"` key in `Cargo.toml` allows `readstat-sys` to discover the include path via the `DEP_ICONV_INCLUDE` environment variable.

### `readstat-wasm` (v0.1.0) — WebAssembly Build
**Path**: `crates/readstat-wasm/`

WebAssembly build of the `readstat` library for parsing SAS `.sas7bdat` files in JavaScript. Compiles the ReadStat C library and the Rust `readstat` library to WebAssembly via the `wasm32-unknown-emscripten` target. Excluded from the Cargo workspace (built separately with Emscripten).

Exports: `read_metadata`, `read_metadata_fast`, `read_data` (CSV), `read_data_ndjson`, `read_data_parquet`, `read_data_feather`, `free_string`, `free_binary`. Not published to crates.io (`publish = false`).

### `readstat-tests` — Integration Tests
**Path**: `crates/readstat-tests/`

30 test modules covering: all SAS data types, 118 date/time/datetime formats, missing values, malformed UTF-8, large pages, CLI subcommands, parallel read/write, Parquet output, CSV output, Arrow migration, row offsets, scientific notation, column selection, skip row count, memory-mapped file reading, byte-slice reading, and SQL queries. Every `sas7bdat` file in the test data directory has both metadata and data reading tests.

Test data lives in `tests/data/*.sas7bdat` (14 datasets). SAS scripts to regenerate test data are in `util/`.

| Dataset | Metadata Test | Data Test |
|---------|:---:|:---:|
| `all_dates.sas7bdat` | ✅ | ✅ |
| `all_datetimes.sas7bdat` | ✅ | ✅ |
| `all_times.sas7bdat` | ✅ | ✅ |
| `all_types.sas7bdat` | ✅ | ✅ |
| `cars.sas7bdat` | ✅ | ✅ |
| `hasmissing.sas7bdat` | ✅ | ✅ |
| `intel.sas7bdat` | ✅ | ✅ |
| `malformed_utf8.sas7bdat` | ✅ | ✅ |
| `messydata.sas7bdat` | ✅ | ✅ |
| `rand_ds_largepage_err.sas7bdat` | ✅ | ✅ |
| `rand_ds_largepage_ok.sas7bdat` | ✅ | ✅ |
| `scientific_notation.sas7bdat` | ✅ | ✅ |
| `somedata.sas7bdat` | ✅ | ✅ |
| `somemiss.sas7bdat` | ✅ | ✅ |

## Build Prerequisites

- Rust (edition 2024)
- libclang (for bindgen)
- Git submodules must be initialized (`git submodule update --init --recursive`)
- On Windows: MSVC toolchain

## Key Architectural Patterns

- **FFI callback pattern**: ReadStat C library calls Rust callbacks (`cb.rs`) during parsing; data accumulates in `ReadStatData` via raw pointer casts
- **Streaming**: default reader streams rows in chunks (10k) to manage memory
- **Parallel processing**: Rayon for parallel reading, Crossbeam channels for reader-writer coordination
- **Column filtering**: optional `--columns` / `--columns-file` flags restrict parsing to selected variables; unselected values are skipped in the `handle_value` callback while row-boundary detection uses the original (unfiltered) variable count
- **Arrow pipeline**: SAS data → typed Arrow builders (direct append in FFI callbacks) → Arrow RecordBatch → output format
- **Multiple I/O strategies**: file path (default), memory-mapped files (`memmap2`), and in-memory byte slices — all feed into the same FFI parsing pipeline
- **Metadata preservation**: SAS variable labels, format strings, and storage widths are persisted as Arrow field metadata, surviving round-trips through Parquet and Feather. See [TECHNICAL.md](TECHNICAL.md#column-metadata-in-arrow-and-parquet) for details.
