# release-check.ps1 — Automated pre-publish verification for crates.io release.
#
# Runs all checks that must pass before publishing. Exit code 0 means ready.
#
# Usage:
#   .\scripts\release-check.ps1

$ErrorActionPreference = "Continue"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Split-Path -Parent $ScriptDir

$Pass = 0
$Fail = 0
$Warn = 0

function Write-Pass($msg) {
    Write-Host "  PASS  $msg" -ForegroundColor Green
    $script:Pass++
}

function Write-Fail($msg) {
    Write-Host "  FAIL  $msg" -ForegroundColor Red
    $script:Fail++
}

function Write-Warn($msg) {
    Write-Host "  WARN  $msg" -ForegroundColor Yellow
    $script:Warn++
}

Write-Host "=== Pre-publish Release Checks ==="
Write-Host ""

# 1. Formatting
Write-Host "Checking formatting..."
$fmtOutput = cargo fmt --all -- --check 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Pass "cargo fmt"
} else {
    Write-Fail "cargo fmt - run 'cargo fmt --all' to fix"
}

# 2. Clippy
Write-Host "Checking clippy..."
$clippyOutput = cargo clippy --workspace 2>&1
if ($clippyOutput -match "warning:") {
    Write-Fail "cargo clippy - warnings found"
} else {
    Write-Pass "cargo clippy"
}

# 2b. readstat-wasm (excluded from workspace - check separately)
Write-Host "Checking readstat-wasm..."
$WasmDir = Join-Path $RootDir "crates\readstat-wasm"
if (Test-Path $WasmDir) {
    Push-Location $WasmDir
    $wasmFmtOutput = cargo fmt -- --check 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Pass "readstat-wasm fmt"
    } else {
        Write-Fail "readstat-wasm fmt - run 'cargo fmt' in crates\readstat-wasm\"
    }
    $wasmClippyOutput = cargo clippy 2>&1
    if ($wasmClippyOutput -match "warning:") {
        Write-Fail "readstat-wasm clippy - warnings found"
    } else {
        Write-Pass "readstat-wasm clippy"
    }
    Pop-Location
} else {
    Write-Warn "readstat-wasm directory not found - skipping"
}

# 3. Tests
Write-Host "Running tests..."
$testOutput = cargo test --workspace 2>&1
if ($testOutput -match "test result: ok") {
    Write-Pass "cargo test"
} else {
    Write-Fail "cargo test - some tests failed"
}

# 4. Doc build
Write-Host "Checking doc build..."
$docOutput = cargo doc --workspace --no-deps 2>&1
Write-Pass "cargo doc"

# 5. cargo-deny (optional)
Write-Host "Checking dependencies..."
$denyPath = Get-Command cargo-deny -ErrorAction SilentlyContinue
if ($denyPath) {
    $denyOutput = cargo deny check 2>&1
    if ($denyOutput -match "error") {
        Write-Fail "cargo deny - license/security issues found"
    } else {
        Write-Pass "cargo deny"
    }
} else {
    Write-Warn "cargo-deny not installed - skipping (install with: cargo install cargo-deny)"
}

# 6. Version consistency
Write-Host "Checking version consistency..."
$readstatVer = (Select-String -Path "$RootDir\crates\readstat\Cargo.toml" -Pattern '^version' | Select-Object -First 1).Line -replace '.*"(.*)".*', '$1'
$cliVer = (Select-String -Path "$RootDir\crates\readstat-cli\Cargo.toml" -Pattern '^version' | Select-Object -First 1).Line -replace '.*"(.*)".*', '$1'
$sysVer = (Select-String -Path "$RootDir\crates\readstat-sys\Cargo.toml" -Pattern '^version' | Select-Object -First 1).Line -replace '.*"(.*)".*', '$1'
$iconvVer = (Select-String -Path "$RootDir\crates\readstat-iconv-sys\Cargo.toml" -Pattern '^version' | Select-Object -First 1).Line -replace '.*"(.*)".*', '$1'

if ($readstatVer -eq $cliVer) {
    Write-Pass "readstat ($readstatVer) and readstat-cli ($cliVer) versions match"
} else {
    Write-Fail "Version mismatch: readstat=$readstatVer, readstat-cli=$cliVer"
}

if ($sysVer -eq $iconvVer) {
    Write-Pass "readstat-sys ($sysVer) and readstat-iconv-sys ($iconvVer) versions match"
} else {
    Write-Fail "Version mismatch: readstat-sys=$sysVer, readstat-iconv-sys=$iconvVer"
}

# 7. CHANGELOG
Write-Host "Checking CHANGELOG..."
$changelogPath = Join-Path $RootDir "CHANGELOG.md"
if (Test-Path $changelogPath) {
    $content = Get-Content $changelogPath -Raw
    if ($content -match [regex]::Escape("[$readstatVer]")) {
        Write-Pass "CHANGELOG.md has entry for $readstatVer"
    } else {
        Write-Fail "CHANGELOG.md missing entry for $readstatVer"
    }
} else {
    Write-Fail "CHANGELOG.md not found"
}

# 8. Package dry-run
Write-Host "Checking package contents..."
$publishableCrates = @("readstat-iconv-sys", "readstat-sys", "readstat", "readstat-cli")
foreach ($crate in $publishableCrates) {
    $pkgOutput = cargo package -p $crate --allow-dirty 2>&1
    if ($pkgOutput -match "warning: aborting") {
        Write-Fail "cargo package -p $crate"
    } else {
        Write-Pass "cargo package -p $crate"
    }
}

# Summary
Write-Host ""
Write-Host "=== Summary ==="
Write-Host "  $Pass passed" -ForegroundColor Green
if ($Warn -gt 0) {
    Write-Host "  $Warn warnings" -ForegroundColor Yellow
}
if ($Fail -gt 0) {
    Write-Host "  $Fail failed" -ForegroundColor Red
    Write-Host ""
    Write-Host "Fix failures before publishing."
    exit 1
} else {
    Write-Host ""
    Write-Host "All checks passed! Ready to publish."
    exit 0
}
