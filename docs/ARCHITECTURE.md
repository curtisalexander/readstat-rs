# Architecture

Rust CLI tool and library that reads SAS binary files (`.sas7bdat`) and converts them to modern columnar formats (Parquet, Feather, CSV, NDJSON). Uses FFI bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library for parsing, and Apache Arrow for in-memory representation and output.

## Workspace Layout

```
readstat-rs/
├── Cargo.toml              # Workspace root (edition 2024, resolver 2)
├── crates/
│   ├── readstat/            # Main binary + library crate
│   ├── readstat-sys/        # FFI bindings to ReadStat C library (bindgen)
│   ├── iconv-sys/           # FFI bindings to iconv (Windows only)
│   └── readstat-tests/      # Integration test suite
└── docs/
```

## Crate Details

### `readstat` (v0.15.0) — Main Crate
**Path**: `crates/readstat/`

Binary with library. CLI uses clap with three subcommands:
- `metadata` — print file metadata (row/var counts, labels, encoding, etc.)
- `preview` — preview first N rows
- `data` — convert to output format (csv, feather, ndjson, parquet)

Key source modules in `crates/readstat/src/`:
| Module | Purpose |
|--------|---------|
| `lib.rs` | Public API exports |
| `cb.rs` | C callback functions for ReadStat (handle_metadata, handle_variable, handle_value) |
| `rs_data.rs` | Data reading, Arrow RecordBatch conversion |
| `rs_metadata.rs` | Metadata extraction, Arrow schema building |
| `rs_parser.rs` | ReadStatParser wrapper around C parser |
| `rs_path.rs` | Path validation, I/O config |
| `rs_var.rs` | Variable types and value handling |
| `rs_write.rs` | Output writers (CSV, Feather, NDJSON, Parquet) |
| `formats.rs` | SAS format detection (118 date/time/datetime formats, regex-based) |
| `err.rs` | Error enum (39 variants mapping to C library errors) |

Key public types:
- `ReadStatData` — holds parsed rows (as `Vec<Vec<ReadStatVar>>`), metadata, Arrow RecordBatch
- `ReadStatMetadata` — file-level metadata (row/var counts, encoding, compression, schema)
- `ReadStatVar` — enum of typed values (String, i8/i16/i32, f32/f64, Date, DateTime variants, Time)
- `ReadStatWriter` — writes output in requested format
- `ReadStatPath` — validated file path with I/O config
- `Reader` — enum: `mem` (full in-memory) or `stream` (chunked, default 10k rows)

Major dependencies: Arrow v57 ecosystem, Parquet (5 compression codecs), Rayon, Crossbeam, clap v4, chrono.

### `readstat-sys` (v0.2.0) — FFI Bindings
**Path**: `crates/readstat-sys/`

`build.rs` compiles ~20 C source files from `vendor/ReadStat/` git submodule via the `cc` crate, then generates Rust bindings with `bindgen`. Platform-specific linking for iconv and zlib:

| Platform | iconv | zlib | Notes |
|----------|-------|------|-------|
| **Windows** (`windows-msvc`) | Static — compiled from vendored `iconv-sys` submodule | Static — compiled via `libz-sys` crate | `iconv-sys` is a `cfg(windows)` dependency; needs `LIBCLANG_PATH` |
| **macOS** (`apple-darwin`) | Dynamic — system `libiconv` | `libz-sys` (uses system zlib) | iconv linked via `cargo:rustc-link-lib=iconv` |
| **Linux** (gnu/musl) | Dynamic — system library | `libz-sys` (prefers system, falls back to source) | No explicit iconv link directives; system linker resolves automatically |

Header include paths are propagated between crates using Cargo's `links` key:
- `iconv-sys` sets `cargo:include=...` which becomes `DEP_ICONV_INCLUDE` in `readstat-sys`
- `libz-sys` sets `cargo:include=...` which becomes `DEP_Z_INCLUDE` in `readstat-sys`

### `iconv-sys` (v0.2.0) — iconv FFI (Windows)
**Path**: `crates/iconv-sys/`

Windows-only (`#[cfg(windows)]`). Compiles libiconv from the `vendor/libiconv-win-build/` git submodule using the `cc` crate, producing a static library. On non-Windows platforms the build script is a no-op. The `links = "iconv"` key in `Cargo.toml` allows `readstat-sys` to discover the include path via the `DEP_ICONV_INCLUDE` environment variable.

### `readstat-tests` — Integration Tests
**Path**: `crates/readstat-tests/`

19 test modules covering: all SAS data types, 118 date/time/datetime formats, missing values, large pages, CLI subcommands, parallel read/write, Parquet output, Arrow migration, row offsets, scientific notation.

Test data lives in `tests/data/*.sas7bdat`. SAS scripts to regenerate test data are in `util/`.

## Build Prerequisites

- Rust (edition 2024)
- libclang (for bindgen)
- Git submodules must be initialized (`git submodule update --init --recursive`)
- On Windows: MSVC toolchain

## Key Architectural Patterns

- **FFI callback pattern**: ReadStat C library calls Rust callbacks (`cb.rs`) during parsing; data accumulates in `ReadStatData` via raw pointer casts
- **Streaming**: default reader streams rows in chunks (10k) to manage memory
- **Parallel processing**: Rayon for parallel reading, Crossbeam channels for reader-writer coordination
- **Arrow pipeline**: SAS data → ReadStatVar vectors → Arrow RecordBatch → output format
