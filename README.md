![readstat-rs](https://github.com/curtisalexander/readstat-rs/workflows/readstat-rs/badge.svg)

# readstat-rs
Command-line tool for working with SAS binary &mdash; `sas7bdat` &mdash; files.

## ReadStat
The command-line tool is developed in Rust and is only possible due to the excellent [ReadStat](https://github.com/WizardMac/ReadStat) library developed by [Evan Miller](https://www.evanmiller.org).

The [ReadStat](https://github.com/WizardMac/ReadStat) repository is included as a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules) within this repository.  In order to build and link, first a [readstat-sys](https://github.com/curtisalexander/readstat-rs/tree/main/readstat-sys) crate is created.  Then the [readstat](https://github.com/curtisalexander/readstat-rs/tree/main/readstat) binary utilizes `readstat-sys` as a dependency.

## Install
\[Mostly\] static binaries for Linux, macOS, and Windows may be found at the [Releases page](https://github.com/curtisalexander/readstat-rs/releases/).

## Build

### Linux and macOS
Building is as straightforward as `cargo build`.

### Windows
Building on Windows requires [LLVM 11](https://releases.llvm.org/download.html) be downloaded and installed.  In addition, the path to `libclang` needs to be set in the environment variable `LIBCLANG_PATH`.  If `LIBCLANG_PATH` is not set then the [readstat-sys build script](https://github.com/curtisalexander/readstat-rs/blob/main/readstat-sys/build.rs) assumes the needed path to be `C:\Program Files\LLVM\lib`.

For details see the following.
- [Check for `LIBCLANG_PATH`](https://github.com/curtisalexander/readstat-rs/blob/main/readstat-sys/build.rs#L78-L83)
- [Building in Github Actions](https://github.com/curtisalexander/readstat-rs/blob/main/.github/workflows/main.yml#L77-L79)

## Run
After [building](#build) or [installing](#install), the binary is invoked using [subcommands](https://docs.rs/structopt/0.3.21/structopt/#external-subcommands).  Currently, the following subcommands have been implemented:
- `metadata` &rarr; writes the following to standard out
    - row count
    - variable count
    - table name
    - table label
    - file encoding
    - format version
    - bitness
    - creation time
    - modified time
    - compression
    - byte order
    - variable names
    - variable type classes
    - variable types
    - variable labels
    - variable formats
- `preview` &rarr; writes the first 10 rows (or optionally the number of rows provided by the user) of parsed data in `csv` format to standard out
- `data` &rarr; writes parsed data in `csv` format to a file

### Metadata
To write metadata to standard out, invoke the following.

```sh
readstat metadata /some/dir/to/example.sas7bdat
```

### Preview Data
To write parsed data (as a `csv`) to standard out, invoke the following (default is to write the first 10 rows).

```sh
readstat preview /some/dir/to/example.sas7bdat
```

To write the first 100 rows of parsed data (as a `csv`) to standard out, invoke the following.

```sh
readstat preview /some/dir/to/example.sas7bdat --rows 100
```

### Data
To write parsed data (as a `csv`) to a file, invoke the following (default is to write all parsed data to the specified file).

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.csv
```

To write the first 100 rows of parsed data (as a `csv`) to a file, invoke the following.

```sh
readstat data /some/dir/to/example.sas7bdat --output /some/dir/to/example.csv --rows 100
```

:memo: The `data` subcommand includes a parameter for `--out-type`, which is the type of file that is to be written.  Currently the only available file type for `--out-type` is `csv` and thus the parameter is elided from the above examples.

### Reader
The `preview` and `data` subcommands include a parameter for `--reader`.  The possible values for `--reader` include the following.
- `mem` &rarr; Parse and read the entire `sas7bdat` into memory before writing to either standard out or a file
- `stream` (default) &rarr; Parse and read at most [10,000 rows](https://github.com/curtisalexander/readstat-rs/blob/main/readstat/src/cb.rs#L15) into memory before writing to disk

**Why is this useful?**
- `mem` is useful for testing purposes
- `stream` is useful for keeping memory usage low for large datasets (and hence is the default)
- In general, users should not need to deviate from the default &mdash; `stream` &mdash; unless they have a specific need
- In addition, by enabling these options as command line parameters [hyperfine](#benchmarking) may be used to benchmark across an assortment of file sizes

### Debug
Debug information is printed to standard out by setting the environment variable `RUST_LOG=debug` before the call to `readstat`.  :warning: This is quite verbose!

```sh
# Linux and macOS
RUST_LOG=debug readstat ...
```

```powershell
# Windows
$env:RUST_LOG="debug"; readstat ...
```

### Help
For full details run with `--help`.

```sh
readstat --help
readstat metadata --help
readstat preview --help
readstat data --help
```

## Testing
To perform unit / integration tests, run the following within the `readstat` directory.

```
cargo test
```

### Valgrind
To ensure no memory leaks, [valgrind](https://valgrind.org/) may be utilized.  For example, to ensure no memory leaks for the test `parse_file_metadata_test`, run the following from within the `readstat` directory.

```
valgrind ./target/debug/deps/parse_file_metadata_test-<hash>
```

## Platform Support
- :heavy_check_mark: Linux   &rarr; successfully builds and runs
    - Principal development environment
- :heavy_check_mark: macOS   &rarr; successfully builds and runs
- :heavy_check_mark: Windows &rarr; successfully builds and runs
    - As of [ReadStat](https://github.com/WizardMac/ReadStat) `1.1.5`, able to build using MSVC in lieu of setting up an msys2 environment
    - [Requires `libclang`](#windows) in order to build as `libclang` is [required by bindgen](https://rust-lang.github.io/rust-bindgen/requirements.html#clang)

## Floating Point Truncation
:warning: Decimal values are truncated to contain only 14 decimal digits!

For example, the number `1.123456789012345` created within SAS would be returned as `1.12345678901234` within Rust.

Why does this happen?  Is this an implementation error?  No, truncation to only 14 decimal digits has been purposely implemented within the Rust code.

As a specific example, when testing with the [cars.sas7bdat](data/README.md) dataset (which was created originally on Windows), the numeric value `4.6` as observed within SAS was being returned as `4.6000000000000005` (16 digits) within Rust.  Values created on Windows with an x64 processor are only accurate to 15 digits.

Only utilizing 14 decimal digits [mirrors the approach](https://github.com/WizardMac/ReadStat/blob/master/src/bin/write/mod_csv.c#L147) of the [ReadStat binary](https://github.com/WizardMac/ReadStat#command-line-usage) when writing to `csv`.

Finally, SAS represents all numeric values in floating-point representation which creates a challenge for **all** parsed numerics!

### Sources
- [How SAS Stores Numeric Values](https://documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=9.4_3.5&docsetId=lrcon&docsetTarget=p0ji1unv6thm0dn1gp4t01a1u0g6.htm&locale=en#n00dmtao82eizen1e6yziw3s31da)
- [Accuracy on x64 Windows Processors](https://documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=9.4_3.5&docsetId=lrcon&docsetTarget=p0ji1unv6thm0dn1gp4t01a1u0g6.htm&locale=en#n0pd8l179ai8odn17nncb4izqq3d)
    - SAS on Windows with x64 processors can only represent 15 digits
- [Floating-point arithmetic may give inaccurate results in Excel](https://docs.microsoft.com/en-us/office/troubleshoot/excel/floating-point-arithmetic-inaccurate-result)

## Benchmarking
Benchmarking performed with [hyperfine](https://github.com/sharkdp/hyperfine).

This example compares the performance of the Rust binary with the performance of the C binary built from the `ReadStat` repository.  In general, hope that performance is fairly close to that of the C binary.

To run, execute the following from within the `readstat` directory.

```powershell
# Windows
hyperfine --warmup 5 "ReadStat_App.exe -f ..\data\cars.sas7bdat ..\data\cars_c.csv" ".\target\release\readstat.exe data ..\data\cars.sas7bdat --output ..\data\cars_rust.csv"
```

:memo: First experiments on Windows are challenging to interpret due to file caching.  Need further research into utilizing the `--prepare` option provided by `hyperfine` on Windows.

```sh
# Linux and macOS
hyperfine --prepare "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches" "readstat -f ../data/cars.sas7bdat ../data/cars_c.csv" "./target/release/readstat data ../data/cars.sas7bdat --output ../data/cars_rust.csv"
```

Other, future, benchmarking may be performed when/if [channels and threads](https://github.com/curtisalexander/readstat-rs/issues/28) are developed.

## Profiling
Profiling performed with [cargo flamegraph](https://github.com/flamegraph-rs/flamegraph).

To run, execute the following from within the `readstat` directory.
```sh
cargo flamegraph --bin readstat -- data ../data/_ahs2019n.sas7bdat --output ../data/_ahs2019n.csv
```

Flamegraph is written to `readstat/flamegraph.svg`.

:memo: Have yet to utilize flamegraphs in order to improve performance.

## Github Actions
Below is the rough `git tag` dance to delete and/or add tags to [trigger Github Actions](https://github.com/curtisalexander/readstat-rs/blob/main/.github/workflows/main.yml#L7-L10).

```sh
# delete local tag
git tag --delete v0.1.0

# delete remote tag
git push --delete origin v0.1.0

# add and commit local changes
git add .
git commit -m "commit msg"

# push local changes to remote
git push

# add local tag
git tag -a v0.1.0 -m "v0.1.0"

# push local tag to remote
git push origin --tags
```

## Goals

### Short Term
Short term, developing the command-line tool was a helpful exercise in binding to a C library using [bindgen](https://rust-lang.github.io/rust-bindgen/) and the [Rust FFI](https://doc.rust-lang.org/nomicon/ffi.html).  It definitely required a review of C pointers (and for which I claim no expertise)!

### Long Term
The long term goals of this repository are uncertain.  Possibilities include:
- Completing and publishing the `readstat-sys` crate that binds to [ReadStat](https://github.com/WizardMac/ReadStat)
- Developing and publishing a Rust library &mdash; `readstat` &mdash; that allows Rust programmers to work with `sas7bdat` files
    - Could implement a custom [serde data format](https://serde.rs/data-format.html) for `sas7bdat` files (implement serialize first and deserialize later (if possible))
- Developing a command line tool that expands the functionality made available by the [readstat](https://github.com/WizardMac/ReadStat#command-line-usage) command line tool
- Developing a command line tool that performs transformations from `sas7bdat` to other file types (via [serde](https://serde.rs/))
    - text
        - `csv`
        - `ndjson`
    - binary
        - `arrow`
        - `parquet`

## Resources
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
