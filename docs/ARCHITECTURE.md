[< Back to README](../README.md)

# Architecture

Rust CLI tool and library that reads SAS binary files (`.sas7bdat`) and converts them to other formats (CSV, Feather, NDJSON, Parquet). Uses FFI bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library for parsing, and Apache Arrow for in-memory representation and output.

**Scope:** The `readstat-sys` crate exposes the full ReadStat C API, which supports SAS (`.sas7bdat`, `.xpt`), SPSS (`.sav`, `.zsav`, `.por`), and Stata (`.dta`). However, the `readstat`, `readstat-cli`, and `readstat-wasm` crates only implement parsing and conversion for **SAS `.sas7bdat` files**. SPSS and Stata support is a possible future addition, but is not planned at this time — the `readstat-sys` bindings already expose the complete SPSS/Stata C API to build on.

## Workspace Layout

```
readstat-rs/
├── Cargo.toml              # Workspace root (edition 2024, resolver 2)
├── crates/
│   ├── readstat/            # Library crate (parse SAS → Arrow, optional format writers)
│   ├── readstat-cli/        # Binary crate (CLI arg parsing, orchestration)
│   ├── readstat-sys/        # FFI bindings to ReadStat C library (bindgen)
│   ├── readstat-iconv-sys/   # FFI bindings to iconv (Windows only)
│   ├── readstat-tests/      # Integration test suite
│   └── readstat-wasm/       # WebAssembly build (excluded from workspace)
├── fuzz/                   # Fuzz testing (standalone Cargo project, cargo-fuzz)
│   ├── fuzz_targets/        # 3 libFuzzer targets
│   └── corpus/              # Seed corpus (14 .sas7bdat files per target)
├── examples/
│   ├── cli-demo/            # CLI conversion demo
│   ├── api-demo/            # REST API servers (Rust + Python)
│   ├── bun-demo/            # WASM usage from Bun/JS
│   ├── web-demo/            # Browser-based viewer and converter
│   └── sql-explorer/        # Browser-based SQL explorer (AlaSQL + WASM)
└── docs/
```

## Crate Details

### `readstat` (v0.29.1) — Library Crate
**Path**: `crates/readstat/`

Pure library for parsing SAS binary files into Arrow RecordBatch format.
Output format writers (CSV, Feather, NDJSON, Parquet) are feature-gated.

Features: `csv`, `feather`, `ndjson`, `parquet`, and `sql` (all enabled by default).

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
| `err.rs` | Error enums: `ReadStatError` (14 variants) plus `ReadStatCError` (41 codes mapping the C library's `readstat_error_t`) |
| `common.rs` | Utility functions |
| `rs_buffer_io.rs` | Buffer I/O operations |

Key public types:
- `ReadStatReader` — primary reusable reader over a path, owned bytes, or mmap; builder options select rows, columns, and chunk size, and `metadata`, `read`, `chunks`, and `visit` choose materialization strategy.
- `ReadStatData` — internal parsing engine that accumulates values directly into typed Arrow builders.
- `ReadStatMetadata` — file-level metadata (row/var counts, encoding, compression, schema)
- `WriteConfig` — validated builder for output path/format/compression
- `ReadStatWriter` — initialized with `(config, schema)`, accepts `RecordBatch` values through `write`, and returns the row count from `finish`
- `OutFormat` — output format enum (Csv, Feather, Ndjson, Parquet)
- `ProgressCallback` — trait for receiving progress updates during parsing

Major dependencies: Arrow v58 ecosystem, Parquet (5 compression codecs, optional), Rayon, chrono, memmap2.

### `readstat-cli` (v0.29.1) — CLI Binary
**Path**: `crates/readstat-cli/`

Binary crate producing the `readstat` CLI tool. Uses clap with three subcommands:
- `metadata` — print file metadata (row/var counts, labels, encoding, etc.)
- `preview` — preview first N rows
- `convert` — convert to CSV, Feather, NDJSON, or Parquet; output extension drives format selection

Owns CLI arg parsing, progress bars, colored output, and reader-writer thread orchestration.
Human metadata formatting and `--columns-file` parsing intentionally live here rather than in the library.

Additional dependencies: clap v4, colored, indicatif, crossbeam, env_logger, path_abs.
The default CLI build includes all output formats but omits the substantially
larger DataFusion SQL engine. The `sql` feature enables `--sql` and `--sql-file`.

### `readstat-sys` (v0.5.2) — FFI Bindings
**Path**: `crates/readstat-sys/`

`build.rs` compiles ~49 C source files from `vendor/ReadStat/` git submodule via the `cc` crate. Rust bindings are pre-generated per `(os, arch)` and checked in at `crates/readstat-sys/src/bindings/bindings_<os>_<arch>.rs`, so default builds need no `libclang` on any platform. Maintainers regenerate via `READSTAT_REGEN_BINDINGS=1 cargo build -p readstat-sys --features buildtime_bindgen` (requires `libclang`; the env var opts in to rewriting the checked-in file — the feature alone only writes to `OUT_DIR`). Exposes the **full** ReadStat API including support for SAS, SPSS, and Stata formats. Platform-specific linking for iconv and zlib:

| Platform | iconv | zlib | Notes |
|----------|-------|------|-------|
| **Windows** (`windows-msvc`, `windows-gnu`) | Static — win-iconv (public domain) compiled by `readstat-iconv-sys` | Static — compiled via `libz-sys` crate | `readstat-iconv-sys` is a `cfg(windows)` dependency; the two flavors use separate pre-gen bindings (MSVC/GNU enum ABIs differ) |
| **macOS** (`apple-darwin`) | Dynamic — system `libiconv` | `libz-sys` (uses system zlib) | iconv linked via `cargo:rustc-link-lib=iconv` |
| **Linux** (gnu/musl) | Dynamic — system library | `libz-sys` (prefers system, falls back to source) | No explicit iconv link directives; system linker resolves automatically |

Header include paths are propagated between crates using Cargo's `links` key:
- `readstat-iconv-sys` sets `cargo:include=...` which becomes `DEP_ICONV_INCLUDE` in `readstat-sys`
- `libz-sys` sets `cargo:include=...` which becomes `DEP_Z_INCLUDE` in `readstat-sys`

### `readstat-iconv-sys` (v0.4.2) — iconv FFI (Windows)
**Path**: `crates/readstat-iconv-sys/`

Windows-target-only (gated on `CARGO_CFG_TARGET_OS == "windows"` so cross-compilation works). Compiles [win-iconv](https://github.com/win-iconv/win-iconv) — a public-domain iconv implementation backed by the Win32 conversion APIs — from the `vendor/win-iconv/` git submodule using the `cc` crate, producing a static library. On non-Windows targets the build script is a no-op. The `links = "iconv"` key in `Cargo.toml` allows `readstat-sys` to discover the include path via the `DEP_ICONV_INCLUDE` environment variable.

### `readstat-wasm` (v0.29.1) — WebAssembly Build
**Path**: `crates/readstat-wasm/`

WebAssembly build of the `readstat` library for parsing SAS `.sas7bdat` files in JavaScript. Compiles the ReadStat C library and the Rust `readstat` library to WebAssembly via the `wasm32-unknown-emscripten` target. Excluded from the Cargo workspace (built separately with Emscripten).

Exports: `read_metadata`, `read_metadata_fast`, bounded NDJSON `read_preview`,
`read_data` (CSV), `read_data_ndjson`, `read_data_parquet`,
`read_data_feather`, reduced row/column variants of all four data exports,
`readstat_last_error`, `free_string`, and `free_binary`.
Browser builds import `env.readstat_progress` to report metadata, preview, and
export stages while native work is running. Not published to crates.io
(`publish = false`).

The WASM package version mirrors `readstat` and `readstat-cli`. Release checks and
tag validation enforce parity, and the `readstat` release replacement updates the
excluded WASM manifest during a version bump.

### SAS Explorer

The static SAS Explorer in `examples/sas-explorer/` processes local files
entirely in a dedicated browser worker. It shows file and variable metadata plus
a bounded row preview, and exports complete datasets or selected variables and
bounded row ranges as CSV, NDJSON, Parquet, or Feather without sending SAS bytes
over the network. The normal Pages workflow
source-builds the canonical WASM module and publishes the app at `/explorer/`
alongside mdBook. Lightweight SQL is the next optional product milestone. See
[SAS-EXPLORER.md](SAS-EXPLORER.md) for the current product and technical plan.

### `readstat-tests` — Integration Tests
**Path**: `crates/readstat-tests/`

33 test modules covering: all SAS data types, 118 date/time/datetime formats, missing values, malformed UTF-8, character encoding conversion (WINDOWS-1251, plus the EUC-TW platform split between GNU/macOS iconv and the vendored win-iconv on Windows), large pages, CLI subcommands, parallel read/write, Parquet output, CSV output, Arrow migration, row offsets, scientific notation, column selection, skip row count, memory-mapped file reading, byte-slice reading, and SQL queries. Every `sas7bdat` file in the test data directory has both metadata and data reading tests.

Test data lives in `tests/data/*.sas7bdat` (16 datasets). Scripts to regenerate test data are in `util/` (SAS programs, plus `create_encoding_variants.py` for the byte-patched encoding variants).

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
| `messydata_1251.sas7bdat` | ✅ | ✅ |
| `messydata_euctw.sas7bdat` | ✅ | ✅ |
| `rand_ds_largepage_err.sas7bdat` | ✅ | ✅ |
| `rand_ds_largepage_ok.sas7bdat` | ✅ | ✅ |
| `scientific_notation.sas7bdat` | ✅ | ✅ |
| `somedata.sas7bdat` | ✅ | ✅ |
| `somemiss.sas7bdat` | ✅ | ✅ |

## Build Prerequisites

- Rust (edition 2024)
- Git submodules must be initialized (`git submodule update --init --recursive`)
- On Windows: MSVC toolchain
- `libclang` is **only** required if regenerating bindings (`--features readstat-sys/buildtime_bindgen`) or building `readstat-wasm`

## Key Architectural Patterns

- **FFI callback pattern**: ReadStat C library calls Rust callbacks (`cb.rs`) during parsing; data accumulates in `ReadStatData` via raw pointer casts
- **Streaming**: `ReadStatReader::visit` uses one ReadStat data-parser invocation
  and rotates bounded Arrow builders at complete row boundaries (10k rows by
  default); `chunks` and `read` are explicit collecting conveniences over it
- **Parallel processing**: the default CLI pipeline feeds the one-pass reader
  through a bounded Crossbeam channel so parsing overlaps writing; default
  CSV/NDJSON workers encode bounded batch groups and Parquet workers encode
  bounded column groups with Rayon. Feather, CSV stdout, SQL output, and
  `--serial-write` conversions use the sequential writer.
- **Column filtering**: optional `--columns` / `--columns-file` flags restrict parsing to selected variables; unselected values are skipped in the `handle_value` callback while row-boundary detection uses the original (unfiltered) variable count
- **Arrow pipeline**: SAS data → typed Arrow builders (direct append in FFI callbacks) → Arrow RecordBatch → output format
- **Multiple I/O strategies**: file path (default), memory-mapped files (`memmap2`), and in-memory byte slices — all feed into the same FFI parsing pipeline
- **SQL**: DataFusion support is enabled by default in the library and opt-in for the CLI. It exposes sync and async APIs. Buffered input supports repeated scans; only channel-backed streaming input is limited to one execution.
- **Metadata preservation**: SAS variable labels, format strings, and storage widths are persisted as Arrow field metadata, surviving round-trips through Parquet and Feather. See [TECHNICAL.md](TECHNICAL.md#column-metadata-in-arrow-and-parquet) for details.
