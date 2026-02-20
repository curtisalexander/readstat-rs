#!/usr/bin/env bash
# Convert the cars.sas7bdat dataset to CSV, NDJSON, Parquet, and Feather
# using the readstat CLI.
#
# Usage:
#   ./convert.sh              # uses readstat from PATH
#   ./convert.sh /path/to/readstat   # uses a specific binary
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INPUT="$REPO_ROOT/crates/readstat-tests/tests/data/cars.sas7bdat"

# Allow the user to pass a custom path to the readstat binary
READSTAT="${1:-readstat}"

if ! command -v "$READSTAT" &>/dev/null; then
  # Try the default cargo build location
  READSTAT="$REPO_ROOT/target/debug/readstat"
  if [[ ! -x "$READSTAT" ]]; then
    READSTAT="$REPO_ROOT/target/release/readstat"
  fi
  if [[ ! -x "$READSTAT" ]]; then
    echo "Error: readstat binary not found."
    echo "Build it first:  cargo build -p readstat-cli"
    exit 1
  fi
fi

echo "Using readstat: $READSTAT"
echo "Input file:     $INPUT"
echo

# --- Metadata ---
echo "=== Metadata ==="
"$READSTAT" metadata "$INPUT" --no-progress
echo

# --- Preview ---
echo "=== Preview (first 5 rows) ==="
"$READSTAT" preview "$INPUT" --rows 5 --no-progress
echo

# --- Convert to CSV ---
echo "Converting to CSV..."
"$READSTAT" data "$INPUT" -o "$SCRIPT_DIR/cars.csv" -f csv --overwrite --no-progress
echo "  -> cars.csv"

# --- Convert to NDJSON ---
echo "Converting to NDJSON..."
"$READSTAT" data "$INPUT" -o "$SCRIPT_DIR/cars.ndjson" -f ndjson --overwrite --no-progress
echo "  -> cars.ndjson"

# --- Convert to Parquet ---
echo "Converting to Parquet..."
"$READSTAT" data "$INPUT" -o "$SCRIPT_DIR/cars.parquet" -f parquet --overwrite --no-progress
echo "  -> cars.parquet"

# --- Convert to Feather ---
echo "Converting to Feather..."
"$READSTAT" data "$INPUT" -o "$SCRIPT_DIR/cars.feather" -f feather --overwrite --no-progress
echo "  -> cars.feather"

echo
echo "Done! All output files written to $SCRIPT_DIR"
echo "Run 'uv run verify_output.py' to validate the output files."
