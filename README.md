![readstat-rs](https://github.com/curtisalexander/readstat-rs/workflows/readstat-rs/badge.svg)

# readstat-rs
Command-line tool to work with SAS binary &mdash; `sas7bdat` &mdash; files.

## ReadStat
The command-line tool is developed in Rust and is only possible due to the excellent [ReadStat](https://github.com/WizardMac/ReadStat) library developed by [Evan Miller](https://www.evanmiller.org).

The [ReadStat](https://github.com/WizardMac/ReadStat) repository is included as a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules) within this repository.  In order to build and link, first a [readstat-sys](https://github.com/curtisalexander/readstat-rs/tree/main/readstat-sys) crate is created.  Then the [readstat](https://github.com/curtisalexander/readstat-rs/tree/main/readstat) binary utilizes `readstat-sys` as a dependency.

## Build

### Linux
Building is as straightforward as `cargo build`.

### macOS
Currently, `readstat` does not build on macOS.  Updates to the [readstat-sys](https://github.com/curtisalexander/readstat-rs/tree/main/readstat-sys) [build.rs](https://github.com/curtisalexander/readstat-rs/blob/main/readstat-sys/build.rs) script are required in order to build.

### Windows
Building on Windows requires [LLVM 11](https://releases.llvm.org/download.html) be downloaded and installed.  In addition, the path to `libclang` needs to be set in the environment variable `LIBCLANG_PATH`.  If `LIBCLANG_PATH` is not set then the [readstat-sys build script](https://github.com/curtisalexander/readstat-rs/blob/main/readstat-sys/build.rs) assumes the needed path to be `C:\Program Files\LLVM\lib`.

For details see the following.
- [Check for `LIBCLANG_PATH`](https://github.com/curtisalexander/readstat-rs/blob/main/readstat-sys/build.rs#L78-L86)
- [Building in Github Actions](https://github.com/curtisalexander/readstat-rs/blob/main/.github/workflows/main.yml#L77-L79)

## Run
After [building](#build), the binary is invoked using [subcommands](https://docs.rs/structopt/0.3.21/structopt/#external-subcommands).  Currently, the following subcommands have been implemented:
- `metadata` &rarr; writes the following to standard out
    - row count
    - variable count
    - variable names
    - variable types
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

:memo: The `data` subcommand can take the parameter `--out-type`.  Currently the only `--out-type` available is `csv` and thus the parameter is elided from the above examples.

### Debug
Debug information can be printed to standard out by setting the environment variable `RUST_LOG=debug` before the call to `readstat`.  :warning: This is quite verbose!

```sh
# Linux
RUST_LOG=debug readstat ...
```

```powershell
# Windows
$env:RUST_LOG="debug"; readstat ...
```

## Testing
To run unit / integration tests, run the following within the `readstat` directory.

```
cargo test
```

### Valgrind
To ensure no memory leaks, [valgrind](https://valgrind.org/) may be utilized.  For example, to ensure no memory leaks for the test `get_row_count_test`, run the following.

```
valgrind ./target/debug/deps/get_row_count_test-<hash>
```

## Platform Support
- Linux &rarr; principal development environment; succesfully builds and runs
- macOS &rarr; currently does not build; see [macOS](#macoS) for more details
- Windows &rarr; successfully builds and runs
    - As of [ReadStat](https://github.com/WizardMac/ReadStat) `1.1.5`, able to build using MSVC in lieu of setting up an msys2 environment
    - [Requires `libclang`](#windows) in order to build as `libclang` is [required by bindgen](https://rust-lang.github.io/rust-bindgen/requirements.html#clang)

## Floating Point Truncation
:warning: Decimal values are truncated to contain only 14 decimal digits!

For example, the number `1.123456789012345` created within SAS would be returned as `1.12345678901234` within Rust.

Why does this happen?  Is this an ementation error?  No, truncation to only 14 decimal digits has been purposely implemented within the Rust code.

As a specific example, when testing with the [cars.sas7bdat](data/README.md) dataset, the numeric value `4.6` as observed within SAS was being returned as `4.6000000000000005` (16 digits) within Rust.  The `cars.sas7bdat` dataset was created on Windows.  Values created on Windows with an x64 processor are only accurate to 15 digits.

Only utilizing 14 decimal digits [mirrors the approach](https://github.com/WizardMac/ReadStat/blob/master/src/bin/write/mod_csv.c#L147) of the [ReadStat binary](https://github.com/WizardMac/ReadStat#command-line-usage) when writing to `csv`.

Finally, SAS represents all numeric values in floating-point representation which creates a challenge for **all** parsed numerics!

### Sources
- [How SAS Stores Numeric Values](https://documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=9.4_3.5&docsetId=lrcon&docsetTarget=p0ji1unv6thm0dn1gp4t01a1u0g6.htm&locale=en#n00dmtao82eizen1e6yziw3s31da)
- [Accuracy on x64 Windows Processors](https://documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=9.4_3.5&docsetId=lrcon&docsetTarget=p0ji1unv6thm0dn1gp4t01a1u0g6.htm&locale=en#n0pd8l179ai8odn17nncb4izqq3d)
    - SAS on Windows with x64 processors can only represent 15 digits
- [Floating-point arithmetic may give inaccurate results in Excel](https://docs.microsoft.com/en-us/office/troubleshoot/excel/floating-point-arithmetic-inaccurate-result)

## Profiling
Profiling performed with [cargo flamegraph](https://github.com/flamegraph-rs/flamegraph).

To run, execute the following.
```sh
cd readstat
cargo flamegraph --bin readstat -- data ../data/ahs2019n.sas7bdat --output ../data/ahs2019n.csv
```

File is written to `readstat/flamegraph.svg`.

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
