[![readstat-rs](https://github.com/curtisalexander/readstat-rs/workflows/readstat-rs/badge.svg)](https://github.com/curtisalexander/readstat-rs/actions)

# readstat-rs
Read, inspect, and convert SAS binary (`.sas7bdat`) files &mdash; from [Rust code](crates/readstat/), the [command line](crates/readstat-cli/), or the [browser](crates/readstat-wasm/). Converts to CSV, Parquet, Feather, and NDJSON using Apache Arrow.

> The original use case was a command-line tool for converting SAS files, but the project has since expanded into a workspace of crates that can be used as a Rust library, a CLI, or compiled to WebAssembly for browser and JavaScript runtimes.

## :key: Dependencies
The command-line tool is developed in Rust and is only possible due to the following _**excellent**_ projects:
- The [ReadStat](https://github.com/WizardMac/ReadStat) C library developed by [Evan Miller](https://www.evanmiller.org)
- The [arrow](https://github.com/apache/arrow-rs) Rust crate developed by the Apache Arrow community

The `ReadStat` library is used to parse and read `sas7bdat` files, and the `arrow` crate is used to convert the read `sas7bdat` data into the [Arrow](https://arrow.apache.org/) memory format. Once in the `Arrow` memory format, the data can be written to other file formats.

> :bulb: **Note:** The ReadStat C library supports SAS, SPSS, and Stata file formats. The [`readstat-sys`](crates/readstat-sys/) crate exposes the **full** ReadStat API &mdash; all 125 functions across all formats. However, the higher-level crates (`readstat`, `readstat-cli`, `readstat-wasm`, `readstat-tests`) currently only implement support for **SAS `.sas7bdat` files**.

## :rocket: Quickstart

Convert the first 50,000 rows of `example.sas7bdat` (by performing the read in parallel) to the file `example.parquet`, overwriting the file if it already exists.
```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.parquet --format parquet --rows 50000 --overwrite --parallel
```

## :package: Install

### Download a Release
\[Mostly\] static binaries for Linux, macOS, and Windows may be found at the [Releases page](https://github.com/curtisalexander/readstat-rs/releases/).

### Setup
Move the `readstat` binary to a known directory and add the binary to the user's [PATH](https://en.wikipedia.org/wiki/PATH_(variable)).

#### Linux & macOS
Ensure the path to `readstat` is added to the appropriate shell configuration file.

#### Windows
For Windows users, path configuration may be found within the Environment Variables menu.  Executing the following from the command line opens the Environment Variables menu for the current user.

```sh
rundll32.exe sysdm.cpl,EditEnvironmentVariables
```

Alternatively, update the user-level `PATH` in PowerShell (replace `C:\path\to\readstat` with the actual directory):

```powershell
$currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
[Environment]::SetEnvironmentVariable("Path", "$currentPath;C:\path\to\readstat", "User")
```

After running the above, restart your terminal for the change to take effect.

### Run
Run the binary.

```sh
readstat --help
```

## :gear: Usage

The binary is invoked using subcommands:
- `metadata` &rarr; writes file and variable metadata to standard out or JSON
- `preview` &rarr; writes the first N rows of parsed data as `csv` to standard out
- `data` &rarr; writes parsed data in `csv`, `feather`, `ndjson`, or `parquet` format to a file

Column metadata &mdash; labels, SAS format strings, and storage widths &mdash; is preserved in Parquet and Feather output as Arrow field metadata. See **[docs/TECHNICAL.md](docs/TECHNICAL.md#column-metadata-in-arrow-and-parquet)** for details.

For the full CLI reference &mdash; including column selection, parallelism, memory considerations, SQL queries, reader modes, and debug options &mdash; see **[docs/USAGE.md](docs/USAGE.md)**.

## :hammer_and_wrench: Build from Source

Clone the repository (with submodules), install platform-specific developer tools, and run `cargo build`. Platform-specific instructions for Linux, macOS, and Windows are in **[docs/BUILDING.md](docs/BUILDING.md)**.

## :computer: [Platform Support](https://doc.rust-lang.org/rustc/platform-support.html)
- :heavy_check_mark: Linux   &rarr; successfully builds and runs
    - [glibc](https://www.gnu.org/software/libc/)
    - [musl](https://www.musl-libc.org/)
- :heavy_check_mark: macOS   &rarr; successfully builds and runs
- :heavy_check_mark: Windows &rarr; successfully builds and runs
    - As of [ReadStat](https://github.com/WizardMac/ReadStat) `1.1.5`, able to build using MSVC in lieu of setting up an msys2 environment
    - [Requires `libclang`](docs/BUILDING.md#windows) in order to build as `libclang` is [required by bindgen](https://rust-lang.github.io/rust-bindgen/requirements.html#clang)

## :books: Documentation

| Document | Description |
|----------|-------------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Crate layout, key types, and architectural patterns |
| [docs/USAGE.md](docs/USAGE.md) | Full CLI reference and examples |
| [docs/BUILDING.md](docs/BUILDING.md) | Clone, build, and linking details per platform |
| [docs/TECHNICAL.md](docs/TECHNICAL.md) | Floating-point precision and date/time handling |
| [docs/TESTING.md](docs/TESTING.md) | Running tests, dataset table, valgrind |
| [docs/BENCHMARKING.md](docs/BENCHMARKING.md) | Criterion benchmarks, hyperfine, and profiling |
| [docs/CI-CD.md](docs/CI-CD.md) | GitHub Actions triggers and artifacts |

## :jigsaw: Workspace Crates

| Crate | Path | Description |
|-------|------|-------------|
| [`readstat`](crates/readstat/) | `crates/readstat/` | Pure library for parsing SAS files into Arrow RecordBatch format. Output writers are feature-gated. |
| [`readstat-cli`](crates/readstat-cli/) | `crates/readstat-cli/` | Binary crate producing the `readstat` CLI tool (arg parsing, progress bars, orchestration). |
| [`readstat-sys`](crates/readstat-sys/) | `crates/readstat-sys/` | Raw FFI bindings to the full ReadStat C library (SAS, SPSS, Stata) via bindgen. |
| [`iconv-sys`](crates/iconv-sys/) | `crates/iconv-sys/` | Windows-only FFI bindings to libiconv for character encoding conversion. |
| [`readstat-tests`](crates/readstat-tests/) | `crates/readstat-tests/` | Integration test suite (29 modules, 13 datasets). |
| [`readstat-wasm`](crates/readstat-wasm/) | `crates/readstat-wasm/` | WebAssembly build for browser/JS usage (excluded from workspace, built with Emscripten). |

For full architectural details, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## :bulb: Examples

The [`examples/`](examples/) directory contains runnable demos showing different ways to use readstat-rs.

| Example | Description |
|---------|-------------|
| [`cli-demo`](examples/cli-demo/) | Convert a `.sas7bdat` file to CSV, NDJSON, Parquet, and Feather using the `readstat` CLI |
| [`bun-demo`](examples/bun-demo/) | Parse a `.sas7bdat` file from JavaScript using the WebAssembly build with Bun |
| [`web-demo`](examples/web-demo/) | Browser-based viewer and converter — upload, preview, and export entirely client-side via WASM |

To use `readstat` as a library in your own Rust project, add the [`readstat`](crates/readstat/) crate as a dependency.

## :link: Resources
The following have been **_incredibly_** helpful while developing!
- [How to not RiiR](http://adventures.michaelfbryan.com/posts/how-not-to-riir/#building-chmlib-sys)
- [Making a *-sys crate](https://kornel.ski/rust-sys-crate)
- [Rust Closures in FFI](https://adventures.michaelfbryan.com/posts/rust-closures-in-ffi/)
- Rust FFI: Microsoft Flight Simulator SDK
    - [Part 1](https://youtu.be/jNNz4h3iIlw)
    - [Part 2](https://youtu.be/ugiR9M16fwg)
- Stack Overflow answers by [Jake Goulding](https://stackoverflow.com/users/155423/shepmaster)
- ReadStat pull request to add [MSVC/Windows support](https://github.com/WizardMac/ReadStat/pull/214)
- [jamovi-readstat](https://github.com/jamovi/jamovi-readstat) [appveyor.yml](https://github.com/jamovi/jamovi-readstat/blob/master/appveyor.yml) file to build ReadStat on Windows
- [Arrow documentation for utilizing ArrayBuilders](https://docs.rs/arrow/latest/arrow/array/trait.ArrayBuilder.html#example)
