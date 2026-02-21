#Requires -Version 5.1
<#
.SYNOPSIS
    Build the mdBook site for readstat-rs.

.DESCRIPTION
    Copies markdown files from their canonical locations (docs/, crate READMEs)
    into book/src/, strips navigation lines that only make sense on GitHub,
    replaces GitHub emoji shortcodes with real Unicode emoji, then runs
    mdbook build.  Optionally builds rustdocs and copies them into the output
    so the book can link to them.

.PARAMETER Docs
    Also build rustdocs (cargo doc) and copy them into the book output.

.EXAMPLE
    .\scripts\build-book.ps1
    .\scripts\build-book.ps1 -Docs
#>

param(
    [switch]$Docs
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$BookSrc  = Join-Path $RepoRoot "book" "src"

# --------------------------------------------------------------------
# 1. Copy and patch docs
# --------------------------------------------------------------------

# GitHub emoji shortcodes -> Unicode emoji
$emojiMap = @{
    ':key:'                    = [char]::ConvertFromUtf32(0x1F511)
    ':bulb:'                   = [char]::ConvertFromUtf32(0x1F4A1)
    ':rocket:'                 = [char]::ConvertFromUtf32(0x1F680)
    ':package:'                = [char]::ConvertFromUtf32(0x1F4E6)
    ':gear:'                   = [char]::ConvertFromUtf32(0x2699) + [char]::ConvertFromUtf32(0xFE0F)
    ':hammer_and_wrench:'      = [char]::ConvertFromUtf32(0x1F6E0) + [char]::ConvertFromUtf32(0xFE0F)
    ':computer:'               = [char]::ConvertFromUtf32(0x1F4BB)
    ':heavy_check_mark:'       = [char]::ConvertFromUtf32(0x2705)
    ':heavy_exclamation_mark:' = [char]::ConvertFromUtf32(0x2757)
    ':books:'                  = [char]::ConvertFromUtf32(0x1F4DA)
    ':jigsaw:'                 = [char]::ConvertFromUtf32(0x1F9E9)
    ':link:'                   = [char]::ConvertFromUtf32(0x1F517)
    ':memo:'                   = [char]::ConvertFromUtf32(0x1F4DD)
    ':warning:'                = [char]::ConvertFromUtf32(0x26A0) + [char]::ConvertFromUtf32(0xFE0F)
}

function Replace-Emoji {
    param([string]$Text)
    foreach ($entry in $emojiMap.GetEnumerator()) {
        $Text = $Text.Replace($entry.Key, $entry.Value)
    }
    return $Text
}

function Patch-Doc {
    param([string]$Path)
    $content = Get-Content -Raw $Path
    $content = $content -replace '(?m)^\[< Back to README\].*\r?\n', ''
    return Replace-Emoji $content
}

Write-Host "Copying docs into book/src/ ..."

# Introduction from root README (strip the CI badge line, replace emoji)
$readme = Get-Content -Raw (Join-Path $RepoRoot "README.md")
$readme = $readme -replace '(?m)^\[!\[readstat-rs\].*\r?\n', ''
$readme = Replace-Emoji $readme
Set-Content -Path (Join-Path $BookSrc "introduction.md") -Value $readme -NoNewline

# docs/ files
$docsMap = @{
    "BUILDING.md"     = "building.md"
    "USAGE.md"        = "usage.md"
    "ARCHITECTURE.md" = "architecture.md"
    "TECHNICAL.md"    = "technical.md"
    "TESTING.md"      = "testing.md"
    "BENCHMARKING.md"   = "benchmarking.md"
    "CI-CD.md"          = "ci-cd.md"
    "MEMORY_SAFETY.md"  = "memory-safety.md"
}

foreach ($entry in $docsMap.GetEnumerator()) {
    $content = Patch-Doc (Join-Path $RepoRoot "docs" $entry.Key)
    Set-Content -Path (Join-Path $BookSrc $entry.Value) -Value $content -NoNewline
}

# Crate READMEs
$cratesDir = Join-Path $BookSrc "crates"
if (-not (Test-Path $cratesDir)) { New-Item -ItemType Directory -Path $cratesDir | Out-Null }

$crateMap = @{
    "readstat"       = "readstat.md"
    "readstat-cli"   = "readstat-cli.md"
    "readstat-sys"   = "readstat-sys.md"
    "iconv-sys"      = "iconv-sys.md"
    "readstat-tests" = "readstat-tests.md"
    "readstat-wasm"  = "readstat-wasm.md"
}

foreach ($entry in $crateMap.GetEnumerator()) {
    Copy-Item (Join-Path $RepoRoot "crates" $entry.Key "README.md") `
              (Join-Path $cratesDir $entry.Value)
}

# --------------------------------------------------------------------
# 2. Build the book
# --------------------------------------------------------------------

Write-Host "Building mdBook ..."
mdbook build (Join-Path $RepoRoot "book")
if ($LASTEXITCODE -ne 0) { throw "mdbook build failed" }

# --------------------------------------------------------------------
# 3. Optionally build rustdocs and copy into the book output
# --------------------------------------------------------------------

if ($Docs) {
    Write-Host "Building rustdocs ..."
    cargo doc --workspace --no-deps --document-private-items
    if ($LASTEXITCODE -ne 0) { throw "cargo doc failed" }

    $BookOut = Join-Path $RepoRoot "target" "book"
    $ApiDir  = Join-Path $BookOut "api"
    Write-Host "Copying rustdocs into $ApiDir ..."
    Copy-Item -Recurse -Force (Join-Path $RepoRoot "target" "doc") $ApiDir
}

Write-Host "Done. Output is in target/book/"
