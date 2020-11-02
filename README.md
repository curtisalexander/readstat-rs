# readstat-rs
Example Rust binary that counts the number of records in a SAS binary file (`sas7bdat`).

## ReadStat
The Rust binary is only possible due to the excellent [ReadStat](https://github.com/WizardMac/ReadStat) library developed by [Evan Miller](https://www.evanmiller.org).  Thus [building](https://github.com/WizardMac/ReadStat#installation) the `ReadStat` library is required to make use of the Rust binary.

The binary binds to `ReadStat` and follows the [reading files](https://github.com/WizardMac/ReadStat#library-usage-reading-files) example in the `ReadStat` repository.

## Run
The binary is run via the following, assuming Rust and cargo [installed and setup](https://rustup.rs/):

```sh
cargo run -- --sas /some/dir/to/example.sas7bdat
```

The record count (along with other, extraneous program information) is written to standard out.

## Goals

### Short Term
Short term the developed binary was a helpful exercise in binding to a C library using [bindgen](https://rust-lang.github.io/rust-bindgen/) and the [Rust FFI](https://doc.rust-lang.org/nomicon/ffi.html).  It definitely required a review of C pointers (and for which I claim no expertise)!

### Long Term
Uncertain of the long term goals of this repository.  Possibilities include:
- Building a Rust library that works with `sas7bdat` files
- Developing a command line tool that expands the functionality made available by the [readstat](https://github.com/WizardMac/ReadStat#command-line-usage) command line tool
- Develop a command line tool that performs transformations from `sas7bdat` to other file types
    - `csv`
    - `json`
    - `parquet`
    - `arrow`
