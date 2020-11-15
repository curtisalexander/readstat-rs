# readstat-rs
Example Rust binary that to work with a SAS binary file (`sas7bdat`).

## ReadStat
The Rust binary is only possible due to the excellent [ReadStat](https://github.com/WizardMac/ReadStat) library developed by [Evan Miller](https://www.evanmiller.org).

The [ReadStat](https://github.com/WizardMac/ReadStat) repository is included as a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules) within this repository.  In order to build and link, first a [readstat-sys](https://github.com/curtisalexander/readstat-rs/tree/main/readstat-sys) crate is created.  Then the [readstat](https://github.com/curtisalexander/readstat-rs/tree/main/readstat) binary utilizes `readstat-sys` as a dependency.

The binary, in part, adapts the [reading files](https://github.com/WizardMac/ReadStat#library-usage-reading-files) example in the `ReadStat` repository.

## Run
After building with `cargo build`, the binary is invoked using [structopt subcommands](https://docs.rs/structopt/0.3.20/structopt/#external-subcommands).  Currently, the following subcommands have been implemented:
- metadata &rarr; writes the following to standard out
    - row count
    - variable count
    - variable names

Debug information can be printed to standard out by setting the environment variable `RUST_LOG=debug` before the call to `readstat`.

```sh
RUST_LOG=debug readstat ...
```

### Metadata
To write metadata to standard out, invoke the following.

```sh
readstat /some/dir/to/example.sas7bdat metadata
```

### Data
To write data (as [tab-separated values](https://en.wikipedia.org/wiki/Tab-separated_values)) to standard out, invoke the following.

```sh
readstat /some/dir/to/example.sas7bdat data
```

To save the resulting `tsv` to a file.

```sh
readstat /some/dir/to/example.sas7bdat data > example.tsv
```

## Testing
To run unit / integration tests, run the following within the `readstat` directory.

```
cargo test
```

To ensure no memory leaks, run [valgrind](https://valgrind.org/).

```
valgrind ./target/debug/deps/get_row_count_test-11793a929ad2468f
```

## Compatibility
- Linux &rarr; developed on Linux and tested there
- macOS &rarr; have not tested; may build and run
- Windows &rarr; does not build; [readstat-sys](https://github.com/curtisalexander/readstat-rs/tree/main/readstat-sys) crate needs work to build on Windows

## Goals

### Short Term
Short term the developed binary was a helpful exercise in binding to a C library using [bindgen](https://rust-lang.github.io/rust-bindgen/) and the [Rust FFI](https://doc.rust-lang.org/nomicon/ffi.html).  It definitely required a review of C pointers (and for which I claim no expertise)!

### Long Term
Uncertain of the long term goals of this repository.  Possibilities include:
- Completing and publishing the `readstat-sys` crate
- Building a Rust library &mdash; `readstat` &mdash; that allows Rust programmers to work with `sas7bdat` files
- Developing a command line tool that expands the functionality made available by the [readstat](https://github.com/WizardMac/ReadStat#command-line-usage) command line tool
- Develop a command line tool that performs transformations from `sas7bdat` to other file types
    - `arrow`
    - `csv`
    - `json`
    - `parquet`

## Resources
The following have been **_incredibly_** helpful while developing.
- [How to not RiiR](http://adventures.michaelfbryan.com/posts/how-not-to-riir/#building-chmlib-sys)
- [Making a *-sys crate](https://kornel.ski/rust-sys-crate)
- [Rust Closures in FFI](https://adventures.michaelfbryan.com/posts/rust-closures-in-ffi/)
- Rust FFI: Microsoft Flight Simulator SDK
    - [Part 1](https://youtu.be/jNNz4h3iIlw)
    - [Part 2](https://youtu.be/ugiR9M16fwg)
- Stack Overflow answers by [Jake Goulding](https://stackoverflow.com/users/155423/shepmaster)