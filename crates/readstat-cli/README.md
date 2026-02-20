# readstat-cli

Binary crate producing the `readstat` CLI tool for converting SAS binary files (`.sas7bdat`) to modern columnar formats.

## Subcommands

- `metadata` — Print file metadata (row/var counts, labels, encoding, format version, etc.)
- `preview` — Preview first N rows as CSV to stdout
- `data` — Convert to output format (csv, feather, ndjson, parquet)

## Key Features

- Column selection (`--columns`, `--columns-file`)
- Streaming reads with configurable chunk size (`--stream-rows`)
- Parallel reading (`--parallel`) and parallel Parquet writing (`--parallel-write`)
- SQL queries via DataFusion (`--sql`, feature-gated)
- Parquet compression settings (`--compression`, `--compression-level`)

For the full CLI reference, see [docs/USAGE.md](../../docs/USAGE.md).
