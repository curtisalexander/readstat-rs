# vendor.ps1 — Switch vendor directories between git submodule (dev) and
# copied files (crates.io publish) modes.
#
# Usage:
#   .\scripts\vendor.ps1 prepare   # Copy vendor files, deinit submodules
#   .\scripts\vendor.ps1 restore   # Remove copies, re-init submodules
#   .\scripts\vendor.ps1 status    # Show current mode

param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("prepare", "restore", "status")]
    [string]$Command
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Split-Path -Parent $ScriptDir
$LockFile = Join-Path $RootDir "vendor-lock.txt"

$ReadStatVendor = Join-Path $RootDir "crates\readstat-sys\vendor\ReadStat"
$IconvVendor = Join-Path $RootDir "crates\iconv-sys\vendor\libiconv-win-build"

function Test-GitRepo {
    $result = git -C $RootDir rev-parse --is-inside-work-tree 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Not inside a git repository"
        exit 1
    }
}

function Save-SubmoduleHashes {
    Write-Host "Recording submodule commit hashes..."
    $lines = @("# Submodule commit hashes - recorded by vendor.ps1 prepare")
    $lines += "# Use these to restore exact versions with vendor.ps1 restore"
    $status = git -C $RootDir submodule status
    foreach ($line in $status) {
        $lines += $line
    }
    $lines | Out-File -FilePath $LockFile -Encoding utf8
    Write-Host "Wrote $LockFile"
}

function Copy-ReadStatFiles {
    $src = $ReadStatVendor
    $tmp = Join-Path $RootDir ".vendor-tmp-readstat"

    Write-Host "Copying ReadStat vendor files..."
    if (Test-Path $tmp) { Remove-Item -Recurse -Force $tmp }
    New-Item -ItemType Directory -Path $tmp -Force | Out-Null

    # Copy LICENSE
    if (Test-Path "$src\LICENSE") {
        Copy-Item "$src\LICENSE" "$tmp\"
    }

    # Copy src/*.c and src/*.h
    $srcDir = Join-Path $tmp "src"
    New-Item -ItemType Directory -Path $srcDir -Force | Out-Null
    Copy-Item "$src\src\*.c" "$srcDir\" -ErrorAction SilentlyContinue
    Copy-Item "$src\src\*.h" "$srcDir\" -ErrorAction SilentlyContinue

    # Copy subdirectories
    foreach ($subdir in @("sas", "spss", "stata", "txt")) {
        $subdirPath = Join-Path "$src\src" $subdir
        if (Test-Path $subdirPath) {
            Copy-Item $subdirPath "$srcDir\" -Recurse
        }
    }

    $count = (Get-ChildItem -Recurse -File $tmp).Count
    Write-Host "  Copied $count files"
    return $tmp
}

function Copy-IconvFiles {
    $src = $IconvVendor
    $tmp = Join-Path $RootDir ".vendor-tmp-iconv"

    Write-Host "Copying libiconv vendor files..."
    if (Test-Path $tmp) { Remove-Item -Recurse -Force $tmp }
    New-Item -ItemType Directory -Path $tmp -Force | Out-Null

    # Copy directories
    foreach ($dir in @("include", "lib", "libcharset", "srclib")) {
        $dirPath = Join-Path $src $dir
        if (Test-Path $dirPath) {
            Copy-Item $dirPath "$tmp\" -Recurse
        }
    }

    # Copy license files
    Copy-Item "$src\COPYING*" "$tmp\" -ErrorAction SilentlyContinue
    if (Test-Path "$src\LICENSE.md") {
        Copy-Item "$src\LICENSE.md" "$tmp\"
    }

    $count = (Get-ChildItem -Recurse -File $tmp).Count
    Write-Host "  Copied $count files"
    return $tmp
}

function Invoke-Prepare {
    Test-GitRepo
    Write-Host "=== Preparing vendor directories for crates.io publish ==="

    # Ensure submodules are initialized
    if (-not (Test-Path (Join-Path $ReadStatVendor "LICENSE"))) {
        Write-Host "Initializing submodules..."
        git -C $RootDir submodule update --init --recursive
    }

    Save-SubmoduleHashes

    $readstatTmp = Copy-ReadStatFiles
    $iconvTmp = Copy-IconvFiles

    Write-Host "Deinitializing submodules..."
    git -C $RootDir submodule deinit --force "crates/readstat-sys/vendor/ReadStat" 2>$null
    git -C $RootDir submodule deinit --force "crates/iconv-sys/vendor/libiconv-win-build" 2>$null

    if (Test-Path $ReadStatVendor) { Remove-Item -Recurse -Force $ReadStatVendor }
    if (Test-Path $IconvVendor) { Remove-Item -Recurse -Force $IconvVendor }

    Move-Item $readstatTmp $ReadStatVendor
    Move-Item $iconvTmp $IconvVendor

    Write-Host ""
    Write-Host "=== Vendor directories prepared for publishing ==="
    $rsCount = (Get-ChildItem -Recurse -File $ReadStatVendor).Count
    $icCount = (Get-ChildItem -Recurse -File $IconvVendor).Count
    Write-Host "ReadStat: $rsCount files"
    Write-Host "libiconv: $icCount files"
    Write-Host ""
    Write-Host "Verify with:"
    Write-Host "  cargo package --list -p readstat-sys --allow-dirty"
    Write-Host "  cargo package --list -p readstat-iconv-sys --allow-dirty"
    Write-Host ""
    Write-Host "To restore submodules after publishing: .\scripts\vendor.ps1 restore"
}

function Invoke-Restore {
    Test-GitRepo
    Write-Host "=== Restoring git submodule development mode ==="

    Write-Host "Removing copied vendor files..."
    if (Test-Path $ReadStatVendor) { Remove-Item -Recurse -Force $ReadStatVendor }
    if (Test-Path $IconvVendor) { Remove-Item -Recurse -Force $IconvVendor }

    Write-Host "Re-initializing submodules..."
    git -C $RootDir submodule update --init --recursive

    if (Test-Path $LockFile) {
        Write-Host ""
        Write-Host "Recorded hashes from last prepare:"
        Get-Content $LockFile | Where-Object { $_ -notmatch "^#" -and $_ -ne "" }
        Write-Host ""
        Write-Host "Current submodule status:"
        git -C $RootDir submodule status
    }

    Write-Host ""
    Write-Host "=== Submodules restored ==="
}

function Invoke-Status {
    Test-GitRepo
    Write-Host "=== Vendor directory status ==="

    # Check ReadStat
    $rsGit = Join-Path $ReadStatVendor ".git"
    if (Test-Path $rsGit) {
        Write-Host "ReadStat: git submodule (development mode)"
    } elseif (Test-Path $ReadStatVendor) {
        $count = (Get-ChildItem -Recurse -File $ReadStatVendor).Count
        Write-Host "ReadStat: copied files (publish mode) - $count files"
    } else {
        Write-Host "ReadStat: NOT PRESENT"
    }

    # Check libiconv
    $icGit = Join-Path $IconvVendor ".git"
    if (Test-Path $icGit) {
        Write-Host "libiconv: git submodule (development mode)"
    } elseif (Test-Path $IconvVendor) {
        $count = (Get-ChildItem -Recurse -File $IconvVendor).Count
        Write-Host "libiconv: copied files (publish mode) - $count files"
    } else {
        Write-Host "libiconv: NOT PRESENT"
    }

    if (Test-Path $LockFile) {
        Write-Host ""
        Write-Host "Lock file: $LockFile"
        Get-Content $LockFile | Where-Object { $_ -notmatch "^#" -and $_ -ne "" }
    }
}

switch ($Command) {
    "prepare" { Invoke-Prepare }
    "restore" { Invoke-Restore }
    "status"  { Invoke-Status }
}
