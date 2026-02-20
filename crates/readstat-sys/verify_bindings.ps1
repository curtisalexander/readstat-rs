# verify_bindings.ps1 — Verify that readstat-sys Rust bindings cover the full ReadStat C API.
#
# Usage:
#   cd crates/readstat-sys
#   .\verify_bindings.ps1           # check latest bindings
#   .\verify_bindings.ps1 -Rebuild  # rebuild first, then check
#
# This script compares:
#   1. Function declarations in vendor/ReadStat/src/readstat.h (the public C API)
#   2. Function definitions in the generated bindings.rs (what Rust can call)
#   3. C source files in vendor/ vs files listed in build.rs
#
# Exit code: 0 if everything matches, 1 if there are gaps.

param(
    [switch]$Rebuild
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Header = Join-Path $ScriptDir "vendor/ReadStat/src/readstat.h"
$BuildRs = Join-Path $ScriptDir "build.rs"
$VendorSrc = Join-Path $ScriptDir "vendor/ReadStat/src"

# Optionally rebuild first
if ($Rebuild) {
    Write-Host "Rebuilding readstat-sys..."
    cargo build -p readstat-sys 2>$null
    Write-Host ""
}

# Find the most recent bindings.rs (largest file = most complete, non-emscripten build)
$TargetDir = Join-Path $ScriptDir "../../target/debug/build"
$Bindings = $null
$BestSize = 0

Get-ChildItem -Path $TargetDir -Filter "bindings.rs" -Recurse -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -match "readstat-sys-" } |
    ForEach-Object {
        $lines = (Get-Content $_.FullName).Count
        if ($lines -gt $BestSize) {
            $Bindings = $_.FullName
            $BestSize = $lines
        }
    }

if (-not $Bindings) {
    Write-Host "Error: No bindings.rs found. Run 'cargo build -p readstat-sys' first." -ForegroundColor Red
    exit 1
}

Write-Host "ReadStat-sys Binding Verification" -ForegroundColor White
Write-Host "=================================="
Write-Host ""
Write-Host "Header:   $Header"
Write-Host "Bindings: $Bindings"
Write-Host "Build.rs: $BuildRs"
Write-Host ""

$ExitCode = 0

# ─── 1. Functions ───────────────────────────────────────────────────────────

$HeaderContent = Get-Content $Header -Raw
$BindingsContent = Get-Content $Bindings -Raw

# Extract function names from header (public API)
$HeaderFuncs = [regex]::Matches($HeaderContent, 'readstat_\w+\(') |
    ForEach-Object { $_.Value -replace '\($', '' } |
    Sort-Object -Unique
$HeaderCount = $HeaderFuncs.Count

# Extract function names from bindings
$BindingFuncs = [regex]::Matches($BindingsContent, 'pub fn readstat_\w+') |
    ForEach-Object { $_.Value -replace '^pub fn ', '' } |
    Sort-Object -Unique
$BindingCount = $BindingFuncs.Count

# Compare
$MissingFuncs = $HeaderFuncs | Where-Object { $_ -notin $BindingFuncs }
$ExtraFuncs = $BindingFuncs | Where-Object { $_ -notin $HeaderFuncs }

Write-Host "1. Functions" -ForegroundColor White
Write-Host "   Header declares:  $HeaderCount functions"
Write-Host "   Bindings contain: $BindingCount functions"

if ($MissingFuncs.Count -eq 0) {
    Write-Host "   All header functions are bound." -ForegroundColor Green
} else {
    Write-Host "   Missing $($MissingFuncs.Count) functions:" -ForegroundColor Red
    $MissingFuncs | ForEach-Object { Write-Host "     - $_" }
    $ExitCode = 1
}

if ($ExtraFuncs.Count -gt 0) {
    Write-Host "   Extra $($ExtraFuncs.Count) functions in bindings (not in header):" -ForegroundColor Yellow
    $ExtraFuncs | ForEach-Object { Write-Host "     - $_" }
}

Write-Host ""

# ─── 2. Types ───────────────────────────────────────────────────────────────

# Enums declared in header (match "typedef enum <name>" declarations)
$HeaderEnums = [regex]::Matches($HeaderContent, 'typedef enum (readstat_\w+)') |
    ForEach-Object { $_.Groups[1].Value } |
    Sort-Object -Unique
$HeaderEnumCount = $HeaderEnums.Count

# Type aliases in bindings
$BindingTypes = [regex]::Matches($BindingsContent, 'pub type (readstat_\w+)') |
    ForEach-Object { $_.Groups[1].Value } |
    Sort-Object -Unique
$BindingTypeCount = $BindingTypes.Count

$MissingTypes = $HeaderEnums | Where-Object { $_ -notin $BindingTypes }

Write-Host "2. Types" -ForegroundColor White
Write-Host "   Header enums:     $HeaderEnumCount"
Write-Host "   Binding types:    $BindingTypeCount (includes enums + typedefs + callback types)"

if ($MissingTypes.Count -eq 0) {
    Write-Host "   All header enums are represented." -ForegroundColor Green
} else {
    Write-Host "   Missing $($MissingTypes.Count) types:" -ForegroundColor Red
    $MissingTypes | ForEach-Object { Write-Host "     - $_" }
    $ExitCode = 1
}

Write-Host ""

# ─── 3. Source Files ────────────────────────────────────────────────────────

# Library source files in vendor (exclude bin/, fuzz/, test/)
$VendorFiles = Get-ChildItem -Path $VendorSrc -Filter "*.c" -Recurse |
    Where-Object { $_.FullName -notmatch '[\\/](bin|fuzz|test)[\\/]' } |
    ForEach-Object { $_.FullName.Replace($VendorSrc, '').TrimStart('\', '/').Replace('\', '/') } |
    Sort-Object
$VendorCount = $VendorFiles.Count

# Source files referenced in build.rs
$BuildContent = Get-Content $BuildRs -Raw
$BuildFiles = @()

# Direct src.join() calls
[regex]::Matches($BuildContent, 'src\.join\("([^"]+)"\)') |
    ForEach-Object { $BuildFiles += $_.Groups[1].Value }

# Subdirectory .join() calls
foreach ($dir in @("sas", "spss", "stata", "txt")) {
    [regex]::Matches($BuildContent, "${dir}\.join\(`"([^`"]+)`"\)") |
        ForEach-Object { $BuildFiles += "$dir/$($_.Groups[1].Value)" }
}

$BuildFiles = $BuildFiles | Sort-Object -Unique
$BuildCount = $BuildFiles.Count

$MissingSources = $VendorFiles | Where-Object { $_ -notin $BuildFiles }

Write-Host "3. C Source Files" -ForegroundColor White
Write-Host "   Vendor library files: $VendorCount"
Write-Host "   build.rs compiles:    $BuildCount"

if ($MissingSources.Count -eq 0) {
    Write-Host "   All vendor library source files are compiled." -ForegroundColor Green
} else {
    Write-Host "   Missing $($MissingSources.Count) source files from build.rs:" -ForegroundColor Red
    $MissingSources | ForEach-Object { Write-Host "     - $_" }
    $ExitCode = 1
}

Write-Host ""

# ─── 4. Function Coverage by Category ──────────────────────────────────────

Write-Host "4. API Coverage by Category" -ForegroundColor White
Write-Host ""
Write-Host ("   {0,-35} {1,5} {2,5} {3}" -f "Category", "Hdr", "Bind", "Status")
Write-Host ("   {0,-35} {1,5} {2,5} {3}" -f ("-" * 35), "-----", "-----", "------")

function Check-Category {
    param(
        [string]$Label,
        [string]$Pattern
    )
    $hdrCount = ($HeaderFuncs | Where-Object { $_ -match $Pattern }).Count
    $bindCount = ($BindingFuncs | Where-Object { $_ -match $Pattern }).Count
    $status = if ($hdrCount -eq $bindCount) { "OK" } else { "MISSING $($hdrCount - $bindCount)"; $script:ExitCode = 1 }
    $color = if ($hdrCount -eq $bindCount) { "Green" } else { "Red" }
    $line = "   {0,-35} {1,5} {2,5} " -f $Label, $hdrCount, $bindCount
    Write-Host -NoNewline $line
    Write-Host $status -ForegroundColor $color
}

Check-Category "Error handling"          '^readstat_error_'
Check-Category "Metadata accessors"      '^readstat_get_'
Check-Category "Value accessors"         '^readstat_(int8|int16|int32|float|double|string)_value$|^readstat_value_|^readstat_type_class$'
Check-Category "Variable accessors"      '^readstat_variable_get_'
Check-Category "Parser lifecycle"        '^readstat_parser_|^readstat_io_free$'
Check-Category "Parser callbacks"        '^readstat_set_(metadata|note|variable|fweight|value|error|progress)_handler$'
Check-Category "Parser I/O handlers"     '^readstat_set_(open|close|seek|read|update)_handler$|^readstat_set_io_ctx$'
Check-Category "Parser config"           '^readstat_set_(file_char|handler_char|row_)'
Check-Category "File parsers (readers)"  '^readstat_parse_'
Check-Category "Schema parsing"          '^readstat_schema_'
Check-Category "Writer lifecycle"        '^readstat_writer_(init|free)$|^readstat_set_data_writer$'
Check-Category "Writer labels"           '^readstat_(add_label|label_)'
Check-Category "Writer variables"        '^readstat_add_variable$|^readstat_variable_set_|^readstat_variable_add_'
Check-Category "Writer notes/strings"    '^readstat_(add_note|add_string|get_string)'
Check-Category "Writer metadata setters" '^readstat_writer_set_'
Check-Category "Writer begin"            '^readstat_begin_writing_'
Check-Category "Writer validation"       '^readstat_validate_'
Check-Category "Writer row insertion"    '^readstat_(begin_row|insert_|end_row|end_writing)$'

Write-Host ""

# ─── Summary ────────────────────────────────────────────────────────────────

if ($ExitCode -eq 0) {
    Write-Host "PASS: readstat-sys has full coverage of the ReadStat C API." -ForegroundColor Green
    Write-Host "  - $HeaderCount/$HeaderCount functions bound"
    Write-Host "  - $HeaderEnumCount/$HeaderEnumCount enum types represented"
    Write-Host "  - $VendorCount/$VendorCount library source files compiled"
} else {
    Write-Host "FAIL: Gaps detected in readstat-sys bindings." -ForegroundColor Red
    Write-Host "  Review the issues above and update build.rs or wrapper.h."
}

exit $ExitCode
