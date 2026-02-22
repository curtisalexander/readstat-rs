# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.19.0]

### Changed
- Bumped `readstat` and `readstat-cli` to 0.19.0 for crates.io release
- Modernized format strings to use inline variable capture (`{var}` instead of `{}", var`)
- Changed `&PathBuf` parameters to `&Path` in public API for idiomatic Rust
- Replaced `Debug`-based `Display` impls with explicit `Display` for `OutFormat` and `ParquetCompression`
- Replaced redundant closures with function references (e.g., `.map(Into::into)`)
- Removed unnecessary `extern crate readstat_sys` from CLI binary
- Added `version = "0.19.0"` to `readstat` dependency in `readstat-cli` for crates.io compatibility

## [0.18.0]

### Added
- DataFusion SQL query support behind `sql` feature flag
- Exposed all ReadStat functionality in readstat-sys 0.3.0
- Focused documentation files broken out from README
- Streaming SQL execution via DataFusion `StreamingTable` integration
- Memory-mapped file reading via `memmap2`
- Support for reading SAS files from in-memory byte slices
- Benchmarks comparing file, mmap, and bytes I/O strategies
- CSV integration tests for header and row count verification
- Miri CI job for detecting undefined behavior in pure-Rust unsafe code
- AddressSanitizer CI jobs on Linux (with C code instrumentation) and macOS
- `READSTAT_SANITIZE_ADDRESS` build flag in `readstat-sys` for targeted ASan instrumentation
- Memory safety documentation (`docs/MEMORY_SAFETY.md`) and mdBook integration

### Changed
- Restructured workspace: extracted `readstat-cli` binary crate from `readstat` library crate
- Feature-gated output format writers (csv, parquet, feather, ndjson) in the library crate
- Arc-wrapped metadata for zero-cost parallel chunk sharing
- Replaced `Vec<ReadStatVar>` intermediate representation with direct Arrow builder pipeline for zero-copy parsing
- Pure-arithmetic f64 rounding (no string formatting or heap allocation)
- Column widths and SAS format strings persisted as Arrow field metadata
- Improved parallelism: local thread pool, ordered reads, bounded-batch writes

### Removed
- Dead code: `ReadStatVar` type, `lexical` dependency, unused dependencies

### Fixed
- CSV file output missing header row due to file truncation during two-step write
- CLI demo convert scripts failing to find `readstat` binary on Windows (missing `.exe` extension)

## [0.17.0] - 2026-02-17

### Added
- Column selection feature with `--columns` and `--columns-file` options (#104)

## [0.16.0] - 2026-02-17

### Added
- Unit tests to core modules (10 to 86 unit tests) (#106)
- Metadata and data tests for all 13 sas7bdat test datasets (#105)
- Comprehensive docs for docs.rs, upgraded CI, and AHS download scripts (#101)

### Changed
- Replaced `unwrap()` calls with typed error handling using `thiserror` (#100)
- Extracted duplicated logic between Preview and Data subcommands (#106)
- Idiomatic Rust improvements: reduced duplication, simplified logic (#106)
- Improved integration test organization with shared helpers (#106)
- Updated README and ARCHITECTURE docs for accuracy (#102)

### Fixed
- Windows CI by updating `install-llvm-action` to v2 (#103)
- Build error with helpful message when `LIBCLANG_PATH` is missing (#101)

### Removed
- Dead code in Preview subcommand (#106)
- Custom serde data format from long-term goals (#102)

## [0.15.0-rc.1] - 2026-02-17

### Added
- AGENTS.md, ARCHITECTURE.md, and CLAUDE.md for LLM context (#96)
- `.gitattributes` for consistent line endings across platforms (#96)
- Microsecond precision support for datetimes and times (#96)
- Missing SPSS C source files to readstat-sys build (#95)

### Changed
- Documented platform-specific iconv and zlib linking (#97)

### Fixed
- Linux musl build by making `libz-sys` an unconditional dependency (#99)

## [0.14.0-rc.2] - 2026-02-16

### Added
- Parallel write support with BufWriter and SpooledTempFile optimization (#93)
- Comprehensive Criterion benchmarking infrastructure (#93)
- Indicatif progress bar showing row counts during processing (#94)
- SAS labels written to Parquet metadata (#94)
- Expanded SAS date/time/datetime format recognition to all 118 formats (#94)

### Changed
- Replaced deprecated `cargo_bin` with `escargot` for cross-platform testing (#93)

## [0.14.0-rc.1] - 2026-01-25

### Added
- `workflow_dispatch` and `repository_dispatch` triggers for CI (#87)

### Changed
- Migrated from `arrow2` to `arrow` crate (#87)
- Upgraded to Rust edition 2024 (#92)

### Fixed
- Datetime milliseconds truncation in CSV output (#91)
- Datetime format detection and variant creation after dependency update (#87)
- Restricted releases to tag pushes only to prevent accidental releases (#88, #89)

### Removed
- Gitpod references (#90)

## [0.13.0] - 2024-10-10

### Added
- Parquet compression support with Snappy, Zstd, Brotli, Gzip, and Lz4Raw (#82)
- Compression level validation for supported codecs

## [0.12.2] - 2023-12-01

### Fixed
- Malformed UTF-8 handling: use lossy conversion for C strings instead of assuming valid UTF-8 (#79, issue #78)
- Updated GitHub Actions checkout and Rust toolchain setup
- LLVM version and Windows release path patterns

## [0.12.1] - 2023-10-13

### Changed
- Updated dependencies
- Clarified installation vs. building instructions in README
- Updated LICENSE for 2023

### Fixed
- Documentation link and header formatting issues

## [0.12.0] - 2023-07-22

### Changed
- Updated all dependencies and corrected for changing APIs (#77)
- Added macOS ARM (aarch64-apple-darwin) build target
- Migrated from deprecated `set-output` to `GITHUB_OUTPUT` in CI
- Updated ReadStat to version 1.1.9
- Migrated to clap 4 (#75)

### Fixed
- ODBC linker error by disabling `all-features` compilation (#74)
- Compiler warnings for unnecessary casts and deprecated functions

## [0.11.0] - 2022-09-18

### Changed
- Restructured project as a Cargo workspace (#73)
- Moved iconv-sys into workspace `Cargo.toml`
- Rebuilt CLI tests to no-op if binary has not been built
- Conditional compilation for iconv-sys (only builds on Windows)

## [0.10] - 2022-08-24

### Changed
- Replaced `arrow-rs` with `arrow2` crate for improved performance and API
- Migrated to clap for CLI argument parsing (replacing structopt)
- Bumped arrow2 through multiple iterations (v0.11, v0.12, v0.13)
- Updated ReadStat submodule
- Major refactoring of internal data structures for arrow2 compatibility
- Significant rewrite of writer to use `ParquetWriter` struct, preserving writer across streaming chunks
- Moved writer into `ReadStatData` struct to avoid cloning chunks
- Set channel to bounded to prevent reader from outpacing writer on large files
- Utilized atomics rather than mutex for global counter
- Switched to 15-digit float precision
- Removed jemalloc dependency

### Added
- CLI tests using polars for round-trip parquet verification
- Builder pattern for `ReadStatData` struct construction
- Metadata as JSON output (`--metadata` flag)
- Row offset option for data access
- Skip row count option for metadata review
- `--overwrite` parameter for output files
- `--no-progress` flag to suppress progress bar

### Fixed
- Streaming for large files (previously read into memory by default)
- File truncation when overwriting existing files
- Parquet writer not appending — now keeps same file across batches
- Scientific notation parsing

## [0.9.4] - 2022-05-19

### Changed
- Updated parquet and arrow crates to 14.0
- Updated ReadStat to version 1.1.8

## [0.9.3] - 2022-03-09

### Changed
- Updated parquet and arrow to 10.0

### Fixed
- Parquet writer creating a new file after every batch instead of appending

## [0.9.2] - 2022-03-02

### Fixed
- Parquet writer not closing properly, causing memory to not flush
- Default stream rows set to 10,000

## [0.9.1] - 2022-02-28

### Fixed
- Memory leak by explicitly dropping `ReadStatData` struct and parser to free memory

## [0.9.0] - 2022-02-26

### Added
- Parallel file reading using rayon with channel-based reader/writer separation
- `--parallel` CLI option for concurrent reads
- Gitpod development environment support

### Changed
- Separated reading from writing using channels for better architecture
- Writer finalization now explicit rather than flag-based
- Rayon thread pool defaults to single thread when `--parallel` is not set

## [0.8.3] - 2022-01-22

### Added
- Metadata output as JSON
- Row offset option for skipping rows
- Option to skip row count when reviewing metadata

### Changed
- Significant refactor: broke `rs.rs` into `data`, `metadata`, `parser`, and `path` modules
- Restructured metadata capture with new `ReadStatMetadata` struct
- Simplified metadata access patterns

## [0.8.2] - 2022-01-19

### Added
- `--overwrite` parameter for output files
- `--no-progress` flag to suppress progress bar
- Metadata tests for `all_types.sas7bdat`

### Changed
- `ReadStatData` struct creation now fully based on builder pattern
- Differentiated between `no_progress` and `no_write` modes

## [0.8.1] - 2022-01-18

### Changed
- Renamed `--json` CLI parameter to `--ndjson` (newline-delimited JSON) for clarity

## [0.8.0] - 2022-01-18

### Added
- NDJSON (newline-delimited JSON) output format
- Integration tests with refactored common test helpers

### Changed
- Bumped arrow and parquet crates
- Utilized jemalloc for musl builds

## [0.7.0] - 2022-01-08

### Added
- `--stream-rows` CLI option to control the number of rows streamed at a time
- `all_types` and `scientific_notation` test datasets
- Reimplemented and expanded integration tests for Apache Arrow-based code

## [0.6.1] - 2022-01-08

### Changed
- Updated arrow and parquet to 6.5

### Fixed
- Removed commented code causing parsing errors from unnecessary string conversions

## [0.6.0] - 2021-11-17

### Changed
- Upgraded to Rust 2021 edition
- Bumped arrow, parquet, and iconv-sys crates
- Updated GitHub Actions versions

## [0.5.2] - 2021-09-15

### Changed
- Updated ReadStat to 1.1.7
- Updated arrow and parquet crates
- Bumped LLVM and `action-gh-release` versions

## [0.5.1] - 2021-08-11

### Fixed
- Incorrect date format detection — all types were incorrectly labeled as `Date32`
- Added additional SAS date formats

## [0.5.0] - 2021-08-09

### Added
- Apache Feather (IPC) output format via `--format feather`

### Changed
- Renamed `--out-type` CLI parameter to `--format`
- Updated ReadStat to v1.1.7-rc1
- Bumped arrow and other dependencies

## [0.4.2] - 2021-07-25

### Changed
- Updated to dev version of ReadStat library
- Added README section on dates, times, and datetimes

## [0.4.1] - 2021-07-25

### Added
- Random test datasets for troubleshooting large SAS files (page sizes > 1 MiB)

### Fixed
- Version information missing from CLI output
- Improved error messaging for unsupported file extensions

## [0.4.0] - 2021-07-22

### Added
- Apache Parquet output format
- Apache Arrow as the in-memory columnar format (replacing custom approach)

### Changed
- Major rewrite to use Arrow for in-memory data representation
- Updated LLVM to version 12
- Updated all cargo dependencies
- Cleaned up progress spinner for write operations (bytes written instead of row count)
- Moved test utilities and data into readstat crate tests directory

## [0.3.2] - 2021-04-14

### Changed
- Improved progress bar formatting for both read and write phases

## [0.3.1] - 2021-04-14

### Fixed
- Off-by-one error in row count display
- Progress bar display improvements — cleaner rendering
- Metadata progress bar/spinner now clears properly
- Updated bindgen to 0.58

## [0.3.0] - 2021-04-12

### Added
- Progress bar and spinner via `indicatif` for tracking read/write progress
- Utility SAS program for creating date and datetime test datasets

### Changed
- Updated ReadStat submodule
- Improved error handling comments for write operations

## [0.2.3] - 2021-01-15

### Changed
- Improved variable format class detection using regex
- Added `lazy_static` and `regex` dependencies for format matching

### Fixed
- Extraneous OR in format-matching regex

## [0.2.2] - 2021-01-14

### Added
- SAS date, datetime, and time type detection based on variable format strings
- Strong typing for dates/datetimes/times (no longer stored as plain strings)
- Conversion logic for SAS datetime doubles to string representations (SAS epoch to Unix epoch)

## [0.2.1] - 2021-01-01

### Added
- Expanded metadata output including column-level metadata
- `chrono` dependency for datetime handling
- Test dataset with all SAS data types

## [0.2.0] - 2020-12-29

### Added
- In-memory reader mode (`--reader mem`) alongside existing streaming mode
- File extension validation for both input and output files

### Changed
- Refactored `ReadStatReader` to simply `Reader`
- Internal buffer rows set to 10,000

## [0.1.8] - 2020-12-27

### Added
- `--reader` CLI parameter to choose between `mem` (in-memory) and `stream` reading modes
- macOS build support with iconv linking

### Changed
- General README cleanup and language improvements

## [0.1.7] - 2020-12-22

### Changed
- Utilize modulo operator for row processing to keep memory in check

## [0.1.6] - 2020-12-22

### Changed
- Updated ReadStat to 1.1.6
- Reduced size of internal buffer (vec) for lower memory usage

## [0.1.5] - 2020-12-22

### Added
- Benchmarking section in README
- Install section with link to releases page
- Utility SAS program to create random test datasets
- Help section in README

### Changed
- Updated ReadStat to 1.1.5

## [0.1.4] - 2020-12-15

### Added
- Initial release of `readstat-rs` CLI tool
- Read SAS7BDAT files using the ReadStat C library via FFI
- CSV output to stdout or file
- `data` and `preview` subcommands
- `--rows` option to limit output rows
- Internal row buffering (10k rows) for write performance
- GitHub Actions CI with cross-platform builds (Linux, macOS, Windows)
