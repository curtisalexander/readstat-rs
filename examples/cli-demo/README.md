# readstat CLI Demo

Demonstrates converting a SAS `.sas7bdat` file to CSV, NDJSON, Parquet, and Feather using the `readstat` command-line tool.

## Quick start

### Linux / macOS

```bash
# Build the CLI (from repo root)
cargo build -p readstat-cli

# Run the conversion script
cd examples/cli-demo
bash convert.sh

# Verify the output files
uv run verify_output.py
```

You can also pass a specific path to the `readstat` binary:

```bash
bash convert.sh /path/to/readstat
```

### Windows (PowerShell)

```powershell
# Build the CLI (from repo root)
cargo build -p readstat-cli

# Run the conversion script
cd examples/cli-demo
./convert.ps1

# Verify the output files
uv run verify_output.py
```

You can also pass a specific path to the `readstat` binary:

```powershell
./convert.ps1 -ReadStat C:\path\to\readstat.exe
```

## What it does

The `convert.sh` (Bash) and `convert.ps1` (PowerShell) scripts:

1. **Displays metadata** for the `cars.sas7bdat` dataset (table name, encoding, row count, variable info)
2. **Previews** the first 5 rows of data
3. **Converts** the dataset to four output formats:
   - `cars.csv` — comma-separated values
   - `cars.ndjson` — newline-delimited JSON
   - `cars.parquet` — Apache Parquet (columnar binary)
   - `cars.feather` — Arrow IPC / Feather (columnar binary)

The `verify_output.py` script validates all output files:

- Checks row and column counts match the expected 1,081 rows x 13 columns
- Verifies column names are correct
- Confirms cross-format consistency (all four formats contain identical data)

## The cars dataset

| Property | Value |
|----------|-------|
| Rows | 1,081 |
| Columns | 13 |
| Source | `crates/readstat-tests/tests/data/cars.sas7bdat` |
| Encoding | WINDOWS-1252 |

Columns: Brand, Model, Minivan, Wagon, Pickup, Automatic, EngineSize, Cylinders, CityMPG, HwyMPG, SUV, AWD, Hybrid

## Expected output

```
Using readstat: /path/to/readstat
Input file:     /path/to/cars.sas7bdat

=== Metadata ===
...

=== Preview (first 5 rows) ===
...

Converting to CSV...
  -> cars.csv
Converting to NDJSON...
  -> cars.ndjson
Converting to Parquet...
  -> cars.parquet
Converting to Feather...
  -> cars.feather

Done! All output files written to /path/to/examples/cli-demo
Run 'uv run verify_output.py' to validate the output files.
```
