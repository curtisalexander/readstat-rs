# readstat-tests

Integration test suite for the [readstat](../readstat/README.md) library and [readstat-cli](../readstat-cli/README.md) binary.

Contains 27 test modules covering all SAS data types, 118 date/time/datetime formats, missing values, large pages, CLI subcommands, parallel read/write, Parquet output, Arrow migration, row offsets, scientific notation, column selection, and skip row count.

Test data lives in `tests/data/*.sas7bdat` (13 datasets). SAS scripts to regenerate test data are in `util/`.

Run with:

```bash
cargo test -p readstat-tests
```
