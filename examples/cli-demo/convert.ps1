# Convert the cars.sas7bdat dataset to CSV, NDJSON, Parquet, and Feather
# using the readstat CLI.
#
# Usage:
#   ./convert.ps1                      # uses readstat from PATH
#   ./convert.ps1 -ReadStat /path/to/readstat  # uses a specific binary
param(
    [string]$ReadStat = "readstat"
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$RepoRoot = Resolve-Path "$ScriptDir/../.."
$Input_ = Join-Path $RepoRoot "crates/readstat-tests/tests/data/cars.sas7bdat"

# Allow the user to pass a custom path to the readstat binary
if (-not (Get-Command $ReadStat -ErrorAction SilentlyContinue)) {
    # Try the default cargo build locations
    $ReadStat = Join-Path $RepoRoot "target/debug/readstat.exe"
    if (-not (Test-Path $ReadStat)) {
        $ReadStat = Join-Path $RepoRoot "target/release/readstat.exe"
    }
    if (-not (Test-Path $ReadStat)) {
        Write-Error "Error: readstat binary not found.`nBuild it first:  cargo build -p readstat-cli"
        exit 1
    }
}

Write-Host "Using readstat: $ReadStat"
Write-Host "Input file:     $Input_"
Write-Host ""

# --- Metadata ---
Write-Host "=== Metadata ==="
& $ReadStat metadata $Input_ --no-progress
Write-Host ""

# --- Preview ---
Write-Host "=== Preview (first 5 rows) ==="
& $ReadStat preview $Input_ --rows 5 --no-progress
Write-Host ""

# --- Convert to CSV ---
Write-Host "Converting to CSV..."
& $ReadStat data $Input_ -o "$ScriptDir/cars.csv" -f csv --overwrite --no-progress
Write-Host "  -> cars.csv"

# --- Convert to NDJSON ---
Write-Host "Converting to NDJSON..."
& $ReadStat data $Input_ -o "$ScriptDir/cars.ndjson" -f ndjson --overwrite --no-progress
Write-Host "  -> cars.ndjson"

# --- Convert to Parquet ---
Write-Host "Converting to Parquet..."
& $ReadStat data $Input_ -o "$ScriptDir/cars.parquet" -f parquet --overwrite --no-progress
Write-Host "  -> cars.parquet"

# --- Convert to Feather ---
Write-Host "Converting to Feather..."
& $ReadStat data $Input_ -o "$ScriptDir/cars.feather" -f feather --overwrite --no-progress
Write-Host "  -> cars.feather"

Write-Host ""
Write-Host "Done! All output files written to $ScriptDir"
Write-Host "Run 'uv run verify_output.py' to validate the output files."
