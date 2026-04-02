[< Back to README](../README.md)

# Testing

To perform unit / integration tests, run the following.

```
cargo test --workspace
```

To run only integration tests:

```
cargo test -p readstat-tests
```

## Datasets
Formally tested (via integration tests) against the following datasets.  See the [README.md](../crates/readstat-tests/tests/data/README.md) for data sources.
- [ ] `ahs2019n.sas7bdat` &rarr; US Census data (download via [download_ahs.sh](../crates/readstat-tests/util/download_ahs.sh) or [download_ahs.ps1](../crates/readstat-tests/util/download_ahs.ps1))
- [X] `all_dates.sas7bdat` &rarr; SAS dataset containing all possible date formats
- [X] `all_datetimes.sas7bdat` &rarr; SAS dataset containing all possible datetime formats
- [X] `all_times.sas7bdat` &rarr; SAS dataset containing all possible time formats
- [X] `all_types.sas7bdat` &rarr; SAS dataset containing all SAS types
- [X] `cars.sas7bdat` &rarr; SAS cars dataset
- [X] `hasmissing.sas7bdat` &rarr; SAS dataset containing missing values
- [X] `intel.sas7bdat`
- [X] `malformed_utf8.sas7bdat` &rarr; SAS dataset with truncated multi-byte UTF-8 characters ([issue #78](https://github.com/curtisalexander/readstat-rs/issues/78))
- [X] `messydata.sas7bdat`
- [X] `rand_ds_largepage_err.sas7bdat` &rarr; Created using [create_rand_ds.sas](../crates/readstat-tests/util/create_rand_ds.sas) with [BUFSIZE](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/ledsoptsref/n0pw7cnugsttken1voc6qo0ye3cg.htm) set to `2M`
- [X] `rand_ds_largepage_ok.sas7bdat` &rarr; Created using [create_rand_ds.sas](../crates/readstat-tests/util/create_rand_ds.sas) with [BUFSIZE](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/ledsoptsref/n0pw7cnugsttken1voc6qo0ye3cg.htm) set to `1M`
- [X] `scientific_notation.sas7bdat` &rarr; Used to test float parsing
- [X] `somedata.sas7bdat` &rarr; Used to test Parquet label preservation
- [X] `somemiss.sas7bdat`

## Fuzz Testing

Fuzz targets live in `fuzz/` (a standalone Cargo project, not a workspace member) and use [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) (libFuzzer). Requires nightly Rust.

### Targets

| Target | What it exercises |
|--------|-------------------|
| `fuzz_read_metadata` | Metadata + variable callbacks, format classification, schema building |
| `fuzz_read_data` | Full metadata→data pipeline including Arrow conversion |
| `fuzz_read_data_filtered` | Column filter index mapping, skipped-variable logic (uses `arbitrary`) |

Each target's corpus is seeded with the 14 test `.sas7bdat` files.

### Running locally

```bash
# Install (one-time)
cargo install cargo-fuzz

# Run a target indefinitely (Ctrl+C to stop)
cargo +nightly fuzz run fuzz_read_metadata

# Run for 10 minutes
cargo +nightly fuzz run fuzz_read_metadata -- -max_total_time=600

# Reproduce a crash
cargo +nightly fuzz run fuzz_read_metadata fuzz/artifacts/fuzz_read_metadata/<crash-file>
```

### CI

Fuzz tests run weekly (Monday 3am UTC) via `.github/workflows/fuzz.yml`. Each target runs for 30 minutes. On crash, a GitHub issue is automatically opened.

## Valgrind
To ensure no memory leaks, [valgrind](https://valgrind.org/) may be utilized.  For example, to ensure no memory leaks for the test `parse_file_metadata_test`, run the following from within the `readstat` directory.

```
valgrind ./target/debug/deps/parse_file_metadata_test-<hash>
```
