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
    Write-Fail "cargo fmt — run 'cargo fmt --all' to fix"
}

# 2. Clippy
Write-Host "Checking clippy..."
cargo clippy --workspace --all-targets --all-features -- -D warnings *>$null
if ($LASTEXITCODE -eq 0) {
    Write-Pass "cargo clippy"
} else {
    Write-Fail "cargo clippy — warnings or errors found"
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
        Write-Fail "readstat-wasm fmt — run 'cargo fmt' in crates\readstat-wasm\"
    }
    cargo clippy --locked --all-targets -- -D warnings *>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Pass "readstat-wasm clippy"
    } else {
        Write-Fail "readstat-wasm clippy — warnings or errors found"
    }
    $emcc = Get-Command emcc -ErrorAction SilentlyContinue
    $targets = rustup target list --installed 2>$null
    if ($emcc -and ($targets -contains "wasm32-unknown-emscripten")) {
        cargo build --locked --target wasm32-unknown-emscripten --release *>$null
        if ($LASTEXITCODE -eq 0) { Write-Pass "readstat-wasm Emscripten build" } else { Write-Fail "readstat-wasm Emscripten build failed" }
    } else {
        Write-Warn "Emscripten/emcc or wasm32-unknown-emscripten unavailable — skipping actual WASM build"
    }
    Pop-Location
} else {
    Write-Warn "readstat-wasm directory not found — skipping"
}

# 2c. MSRV — the workspace must at least type-check on the exact toolchain
# declared in `[workspace.package] rust-version`. Skipped (with a warning) if
# that toolchain isn't installed; CI's `msrv` job enforces it regardless.
$Msrv = (Select-String -Path (Join-Path $RootDir "Cargo.toml") -Pattern '^rust-version\s*=\s*"([^"]+)"' | Select-Object -First 1).Matches[0].Groups[1].Value
Write-Host "Checking MSRV ($Msrv)..."
$toolchains = rustup toolchain list 2>$null
if ($toolchains -match "^$Msrv") {
    cargo "+$Msrv" check --workspace --all-targets *>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Pass "MSRV $Msrv check"
    } else {
        Write-Fail "MSRV $Msrv check — workspace does not build on rust-version; bump rust-version or fix"
    }
} else {
    Write-Warn "MSRV toolchain $Msrv not installed — skipping (install: rustup toolchain install $Msrv)"
}

# 3. Tests
Write-Host "Running tests..."
cargo check --workspace --all-targets --all-features *>$null
if ($LASTEXITCODE -eq 0) { Write-Pass "workspace all-feature/all-target check" } else { Write-Fail "workspace all-feature/all-target check" }
cargo check -p readstat --no-default-features *>$null
$readstatMinimal = $LASTEXITCODE
cargo check -p readstat-cli --no-default-features *>$null
if (($readstatMinimal -eq 0) -and ($LASTEXITCODE -eq 0)) { Write-Pass "minimal feature builds" } else { Write-Fail "minimal feature builds" }
cargo test --workspace --all-features *>$null
if ($LASTEXITCODE -eq 0) {
    Write-Pass "cargo test"
} else {
    Write-Fail "cargo test — some tests failed"
}

# 4. Doc build
Write-Host "Checking doc build..."
$lockContent = Get-Content (Join-Path $RootDir "Cargo.lock") -Raw
$lockstep = $true
foreach ($crate in @("arrow", "parquet")) {
    $majors = $lockContent -split '\[\[package\]\]' | Where-Object { $_ -match "(?m)^name = `"$crate`"$" } | ForEach-Object {
        if ($_ -match '(?m)^version = "(\d+)\.') { $Matches[1] }
    } | Sort-Object -Unique
    if (@($majors).Count -gt 1) { $lockstep = $false }
}
if ($lockstep) { Write-Pass "Arrow/DataFusion lockstep" } else { Write-Fail "Arrow/DataFusion lockstep" }
cargo doc --workspace --all-features --no-deps *>$null
if ($LASTEXITCODE -eq 0) {
    Write-Pass "cargo doc"
} else {
    Write-Fail "cargo doc — build failed"
}
& (Join-Path $ScriptDir "build-book.ps1") *>$null
if ($LASTEXITCODE -eq 0) { Write-Pass "mdBook" } else { Write-Fail "mdBook build failed" }

Write-Host "Checking advertised examples..."
cargo check --manifest-path (Join-Path $RootDir "examples\api-demo\rust-server\Cargo.toml") *>$null
if ($LASTEXITCODE -eq 0) { Write-Pass "Rust API server" } else { Write-Fail "Rust API server" }
cargo check --manifest-path (Join-Path $RootDir "examples\api-demo\python-server\readstat_py\Cargo.toml") *>$null
if ($LASTEXITCODE -eq 0) { Write-Pass "PyO3 extension" } else { Write-Fail "PyO3 extension" }

# 5. cargo-deny (optional)
Write-Host "Checking dependencies..."
$denyPath = Get-Command cargo-deny -ErrorAction SilentlyContinue
if ($denyPath) {
    cargo deny check *>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Pass "cargo deny"
    } else {
        Write-Fail "cargo deny — license/security issues found"
    }
} else {
    Write-Warn "cargo-deny not installed — skipping (install with: cargo install cargo-deny)"
}

# 6. Version consistency
Write-Host "Checking version consistency..."
$readstatVer = (Select-String -Path "$RootDir\crates\readstat\Cargo.toml" -Pattern '^version' | Select-Object -First 1).Line -replace '.*"(.*)".*', '$1'
$cliVer = (Select-String -Path "$RootDir\crates\readstat-cli\Cargo.toml" -Pattern '^version' | Select-Object -First 1).Line -replace '.*"(.*)".*', '$1'
$wasmVer = (Select-String -Path "$RootDir\crates\readstat-wasm\Cargo.toml" -Pattern '^version' | Select-Object -First 1).Line -replace '.*"(.*)".*', '$1'
$wasmPkgVer = (Get-Content "$RootDir\crates\readstat-wasm\pkg\package.json" -Raw | ConvertFrom-Json).version
$sysVer = (Select-String -Path "$RootDir\crates\readstat-sys\Cargo.toml" -Pattern '^version' | Select-Object -First 1).Line -replace '.*"(.*)".*', '$1'
$iconvVer = (Select-String -Path "$RootDir\crates\readstat-iconv-sys\Cargo.toml" -Pattern '^version' | Select-Object -First 1).Line -replace '.*"(.*)".*', '$1'

if (($readstatVer -eq $cliVer) -and ($readstatVer -eq $wasmVer) -and ($wasmVer -eq $wasmPkgVer)) {
    Write-Pass "readstat, readstat-cli, readstat-wasm, and WASM package versions match ($readstatVer)"
} else {
    Write-Fail "Version mismatch: readstat=$readstatVer, readstat-cli=$cliVer, readstat-wasm=$wasmVer, wasm-package=$wasmPkgVer"
}

# readstat-sys and readstat-iconv-sys version independently (each is bumped
# only when it changes); the real constraint is that readstat-sys's declared
# dependency requirement matches readstat-iconv-sys's actual version.
$sysIconvDep = (Select-String -Path "$RootDir\crates\readstat-sys\Cargo.toml" -Pattern 'readstat-iconv-sys' | Where-Object { $_.Line -match 'version' } | Select-Object -First 1).Line -replace '.*version\s*=\s*"(.*?)".*', '$1'
if ($sysIconvDep -eq $iconvVer) {
    Write-Pass "readstat-sys depends on readstat-iconv-sys $sysIconvDep (matches)"
} else {
    Write-Fail "readstat-sys depends on readstat-iconv-sys $sysIconvDep but current is $iconvVer"
}

# Check that readstat depends on the current readstat-sys version
$readstatSysDep = (Select-String -Path "$RootDir\crates\readstat\Cargo.toml" -Pattern 'readstat-sys' | Where-Object { $_.Line -match 'version' } | Select-Object -First 1).Line -replace '.*version\s*=\s*"(.*?)".*', '$1'
if ($readstatSysDep -eq $sysVer) {
    Write-Pass "readstat depends on readstat-sys $readstatSysDep (matches)"
} else {
    Write-Fail "readstat depends on readstat-sys $readstatSysDep but current is $sysVer"
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
    cargo package -p $crate --allow-dirty --list *>$null
    if ($LASTEXITCODE -eq 0) { Write-Pass "cargo package -p $crate --list" } else { Write-Fail "cargo package -p $crate --list" }
    # Test the exit code, not a grepped string ("warning: aborting" is no
    # longer printed by modern cargo, so matching it fails open).
    #
    # Exception: before the FIRST publish, packaging any crate with a path
    # dependency fails with "no matching package named `<dep>` found" because
    # the dependency isn't on crates.io yet. That's expected — downgrade it to
    # a warning. Real packaging errors still fail.
    $pkgOutput = cargo package -p $crate --locked --allow-dirty 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Pass "cargo package -p $crate"
    } elseif ($pkgOutput -match "no matching package named") {
        Write-Warn "cargo package -p $crate — path dependency not on crates.io yet (expected before first publish)"
    } else {
        Write-Fail "cargo package -p $crate"
    }
}

# 9. Vendor status
Write-Host "Checking vendor status..."
$vendorScript = Join-Path $ScriptDir "vendor.ps1"
if (Test-Path $vendorScript) {
    & $vendorScript status *>$null
    if ($LASTEXITCODE -eq 0) { Write-Pass "vendor status" } else { Write-Fail "vendor status could not be verified" }
} else {
    Write-Fail "vendor.ps1 not found"
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
