# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Column selection feature with `--columns` and `--columns-file` options (#104)
- Unit tests to core modules (10 to 86 unit tests) (#106)
- Metadata and data tests for all 13 sas7bdat test datasets (#105)
- Comprehensive docs for docs.rs, upgraded CI, and AHS download scripts (#101)
- AGENTS.md, ARCHITECTURE.md, and CLAUDE.md for LLM context (#96)
- `.gitattributes` for consistent line endings across platforms (#96)
- Microsecond precision support for datetimes and times (#96)
- Parallel write support with BufWriter and SpooledTempFile optimization (#93)
- Comprehensive Criterion benchmarking infrastructure (#93)
- Indicatif progress bar showing row counts during processing (#94)
- SAS labels written to Parquet metadata (#94)
- Expanded SAS date/time/datetime format recognition to all 118 formats (#94)
- `workflow_dispatch` and `repository_dispatch` triggers for CI (#87)

### Changed
- Migrated from `arrow2` to `arrow` crate (#87)
- Upgraded to Rust edition 2024 (#92)
- Replaced `unwrap()` calls with typed error handling using `thiserror` (#100)
- Extracted duplicated logic between Preview and Data subcommands (#106)
- Idiomatic Rust improvements: reduced duplication, simplified logic (#106)
- Improved integration test organization with shared helpers (#106)
- Replaced deprecated `cargo_bin` with `escargot` for cross-platform testing (#93)
- Updated README and ARCHITECTURE docs for accuracy (#102)
- Documented platform-specific iconv and zlib linking (#97)

### Fixed
- Off-by-one bug in Preview subcommand (#106)
- Windows CI by updating `install-llvm-action` to v2 (#103)
- Linux musl build by making `libz-sys` an unconditional dependency (#99)
- Missing SPSS C source files in readstat-sys build (#95)
- Datetime milliseconds truncation in CSV output (#91)
- Datetime format detection and variant creation after dependency update (#87)
- Restricted releases to tag pushes only to prevent accidental releases (#88, #89)
- Build error with helpful message when `LIBCLANG_PATH` is missing (#101)

### Removed
- Dead code in Preview subcommand (#106)
- Gitpod references (#90)
- Custom serde data format from long-term goals (#102)

## [0.13.0] - 2024-10-10

### Added
- Parquet compression support with Snappy, Zstd, Brotli, Gzip, and Lz4Raw (#82)
- Compression level validation for supported codecs

### Changed
- Version bump to 0.13.0

## 2023-12-01

### Fixed
- Malformed UTF-8 handling: use lossy conversion for C strings instead of assuming valid UTF-8 (#79, issue #78)
- Updated GitHub Actions checkout and Rust toolchain setup
- LLVM version and Windows release path patterns

## 2023-07-22

### Changed
- Updated all dependencies and corrected for changing APIs (#77)
- Added macOS ARM (aarch64-apple-darwin) build target
- Migrated from deprecated `set-output` to `GITHUB_OUTPUT` in CI
- Bumped version and updated LICENSE for 2023

### Fixed
- Compiler warnings for unnecessary casts and deprecated functions

## 2022-10-12

### Changed
- Migrated to clap 4 (#75)

## 2022-09-29

### Fixed
- ODBC linker error by disabling `all-features` compilation (#74)

## 2022-09-18

### Changed
- Restructured project as a Cargo workspace (#73)
- Moved iconv-sys into workspace Cargo.toml
- Rebuilt CLI tests to no-op if binary has not been built
