#!/usr/bin/env bash
# demo.sh — Short, self-contained tour of the readstat CLI.
#
# Reads one bundled test file (cars.sas7bdat) and shows off a handful of
# headline features: metadata (human + JSON), preview, column projection,
# and conversion to a compressed columnar format. Optionally a SQL beat.
#
# Double duty:
#   * Recorded as an ascii screencast for the README (see docs/demo.tape).
#   * Runs non-interactively in CI as a happy-path CLI smoke test.
#
# Usage:
#   ./scripts/demo.sh              # paced for recording (typewriter pauses)
#   DEMO_SPEED=0 ./scripts/demo.sh # no pauses — CI smoke test
#   DEMO_SQL=1 ./scripts/demo.sh   # include the SQL beat (needs --features sql)
#
# Environment:
#   DEMO_SPEED  Seconds to pause before/after each command (default 1).
#               Set to 0 for an instant, non-interactive run.
#   DEMO_SQL    If "1", include the SQL aggregation beat. Requires a binary
#               built with `--features sql`.
#   READSTAT    Override the binary/command used (whitespace-separated, e.g.
#               "cargo run -q -p readstat-cli --"). Default: release binary if
#               present, else a cargo-run fallback.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DEMO_SPEED="${DEMO_SPEED:-1}"
DEMO_SQL="${DEMO_SQL:-0}"

# --- Resolve the binary into an array (so pipes & word-splitting work) -------
# Prefer a release build for clean, fast output; fall back to cargo run.
if [ -n "${READSTAT:-}" ]; then
    read -ra RS <<< "$READSTAT"
elif [ -x "$ROOT_DIR/target/release/readstat" ]; then
    RS=("$ROOT_DIR/target/release/readstat")
else
    RS=(cargo run -q -p readstat-cli --)
fi

DATA="$ROOT_DIR/crates/readstat-tests/tests/data/cars.sas7bdat"

# --- Working directory (so output artifacts don't dirty the tree) -----------
WORK_DIR="$(mktemp -d)"
cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

cp "$DATA" "$WORK_DIR/cars.sas7bdat"
cd "$WORK_DIR"

# --- Presentation helpers ----------------------------------------------------
BOLD='\033[1m'
CYAN='\033[0;36m'
DIM='\033[2m'
NC='\033[0m'

pause() { [ "$DEMO_SPEED" != "0" ] && sleep "$DEMO_SPEED"; return 0; }

# Print a "$ <command>" prompt line, then pause (typewriter beat).
# The actual command is run on the following line(s) by the caller, so
# pipelines (| head, | jq) read naturally.
prompt() {
    printf "${CYAN}\$${NC} ${BOLD}%s${NC}\n" "$1"
    pause
}

banner() { printf "\n${DIM}# %s${NC}\n" "$*"; }

# --- The demo ----------------------------------------------------------------

banner "1. Human-readable file + variable metadata"
prompt "readstat metadata cars.sas7bdat"
"${RS[@]}" metadata cars.sas7bdat
echo; pause

banner "2. The same metadata as JSON (machine-readable)"
prompt "readstat metadata cars.sas7bdat --as-json | head -c 400"
"${RS[@]}" metadata cars.sas7bdat --as-json | head -c 400
echo; echo; pause

banner "3. Preview the first 5 rows as CSV"
prompt "readstat preview cars.sas7bdat --rows 5"
"${RS[@]}" preview cars.sas7bdat --rows 5
echo; pause

banner "4. Project just the columns you care about (text + numeric)"
prompt "readstat preview cars.sas7bdat --columns Brand,Model,EngineSize,Cylinders,CityMPG,HwyMPG --rows 5"
"${RS[@]}" preview cars.sas7bdat --columns Brand,Model,EngineSize,Cylinders,CityMPG,HwyMPG --rows 5
echo; pause

banner "5. Convert to compressed Parquet"
prompt "readstat data cars.sas7bdat -o cars.parquet -f parquet --compression zstd --overwrite"
"${RS[@]}" data cars.sas7bdat -o cars.parquet -f parquet --compression zstd --overwrite
prompt "ls -lh cars.parquet"
ls -lh cars.parquet
echo; pause

banner "6. Convert a few rows to NDJSON"
prompt "readstat data cars.sas7bdat -o cars.ndjson -f ndjson --rows 3 --overwrite"
"${RS[@]}" data cars.sas7bdat -o cars.ndjson -f ndjson --rows 3 --overwrite
prompt "cat cars.ndjson"
cat cars.ndjson
echo; pause

if [ "$DEMO_SQL" = "1" ]; then
    banner "7. Query with SQL (requires a --features sql build)"
    SQL='SELECT "Brand", ROUND(AVG("CityMPG"),1) AS avg_city_mpg FROM cars GROUP BY "Brand" ORDER BY avg_city_mpg DESC LIMIT 8'
    prompt "readstat data cars.sas7bdat --sql '$SQL' -o top.csv --overwrite"
    "${RS[@]}" data cars.sas7bdat --sql "$SQL" -o top.csv --overwrite
    prompt "cat top.csv"
    cat top.csv
    echo; pause
fi

banner "Done."
