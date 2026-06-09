#Requires -Version 7.0
<#
.SYNOPSIS
    Report outdated workspace dependencies with publish dates.

.DESCRIPTION
    Surfaces, from `cargo update --dry-run` + the crates.io API:
      1. Held-back COMPATIBLE updates — a newer semver-compatible version exists
         but the lock is pinned (often by a transitive constraint). Subject to
         the quarantine check and applied by -Apply.
      2. MAJOR updates — a newer semver-INCOMPATIBLE version is available. These
         are reported but NEVER applied automatically: bumping them requires
         editing the version requirement in Cargo.toml by hand.
      3. A dedicated `bindgen` advisory. bindgen is exact-pinned ("=x.y.z")
         because its output drives the checked-in per-target FFI bindings; this
         script reports a newer bindgen if one exists and prints how to take it.

    The quarantine flags versions published < QuarantineDays ago as risky,
    giving scanners (cargo-audit, cargo-deny, RustSec) time to flag compromised
    releases.

.PARAMETER QuarantineDays
    Minimum age in days a crate version must have before it is considered
    safe to adopt. Default: 7.

.PARAMETER Apply
    Pull safe COMPATIBLE updates into Cargo.lock. MAJOR bumps and bindgen are
    never applied automatically (see their advisories).

.EXAMPLE
    ./scripts/check-updates.ps1
    ./scripts/check-updates.ps1 -Apply
    ./scripts/check-updates.ps1 -QuarantineDays 3 -Apply
#>
param(
    [int]$QuarantineDays = 7,
    [switch]$Apply
)

$ErrorActionPreference = 'Stop'

# Resolve repo root so the script works from any directory.
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $RepoRoot

# ── Version helpers ───────────────────────────────────────────────────────────

function Convert-VerParts([string]$v) {
    $core = ($v -split '[-+]')[0]
    $p = $core -split '\.'
    $maj = if ($p.Count -ge 1 -and $p[0] -match '^\d+$') { [int]$p[0] } else { 0 }
    $min = if ($p.Count -ge 2 -and $p[1] -match '^\d+$') { [int]$p[1] } else { 0 }
    $pat = if ($p.Count -ge 3 -and $p[2] -match '^\d+$') { [int]$p[2] } else { 0 }
    return , @($maj, $min, $pat)
}

# True if $a is strictly newer than $b (pre-release/build ignored).
function Test-VersionGreater([string]$a, [string]$b) {
    $pa = Convert-VerParts $a; $pb = Convert-VerParts $b
    for ($i = 0; $i -lt 3; $i++) {
        if ($pa[$i] -gt $pb[$i]) { return $true }
        if ($pa[$i] -lt $pb[$i]) { return $false }
    }
    return $false
}

# True if $avail is semver-caret-compatible with $cur (Cargo's default `^`):
#   x>=1 → same major;  0.y (y>=1) → same minor;  0.0.z → same patch.
function Test-CaretCompatible([string]$cur, [string]$avail) {
    $c = Convert-VerParts $cur; $a = Convert-VerParts $avail
    if ($c[0] -ne 0) { return ($c[0] -eq $a[0]) }
    elseif ($c[1] -ne 0) { return ($a[0] -eq 0 -and $c[1] -eq $a[1]) }
    else { return ($a[0] -eq 0 -and $a[1] -eq 0 -and $c[2] -eq $a[2]) }
}

$Headers = @{ 'User-Agent' = 'readstat-rs-check-updates (https://github.com/curtisalexander/readstat-rs)' }

# ── Gather candidates from cargo ──────────────────────────────────────────────
Write-Host "`nChecking for outdated dependencies…" -ForegroundColor Blue
Write-Host ""

$raw = cargo update --dry-run --verbose 2>&1 | Out-String

$compat = [System.Collections.Generic.List[object]]::new()   # compatible, held back
$major = [System.Collections.Generic.List[object]]::new()     # major / incompatible
$bindgenCargoAvail = $null

foreach ($line in ($raw -split "`n")) {
    if ($line -notmatch '\(available:') { continue }
    if ($line -match '(?:Updating|Unchanged)\s+(\S+)\s+v(\S+)') {
        $name = $Matches[1]; $cur = $Matches[2]
    }
    else { continue }
    if ($line -match '\(available:\s*v([^)]+)\)') { $avail = $Matches[1].Trim() } else { continue }

    if ($name -eq 'bindgen') { $bindgenCargoAvail = $avail; continue }

    $obj = [PSCustomObject]@{ Name = $name; Current = $cur; Available = $avail }
    if (Test-CaretCompatible $cur $avail) { $compat.Add($obj) } else { $major.Add($obj) }
}

# ── bindgen advisory check (independent of cargo, since it is exact-pinned) ────
$bindgenPin = $null
$pinMatch = Select-String -Path (Join-Path $RepoRoot 'Cargo.toml') `
    -Pattern '^\s*bindgen\s*=\s*"=([0-9][0-9A-Za-z.+-]*)"' | Select-Object -First 1
if ($pinMatch) { $bindgenPin = $pinMatch.Matches[0].Groups[1].Value }

$bindgenLatest = $null
if ($bindgenPin) {
    try {
        $resp = Invoke-RestMethod -Uri 'https://crates.io/api/v1/crates/bindgen' -Headers $Headers -TimeoutSec 10
        $bindgenLatest = if ($resp.crate.max_stable_version) { $resp.crate.max_stable_version } else { $resp.crate.max_version }
    }
    catch { $bindgenLatest = $null }
    if ($bindgenCargoAvail -and (Test-VersionGreater $bindgenCargoAvail ($bindgenLatest ?? '0'))) {
        $bindgenLatest = $bindgenCargoAvail
    }
}

$bindgenHasUpdate = ($bindgenPin -and $bindgenLatest -and (Test-VersionGreater $bindgenLatest $bindgenPin))

if ($compat.Count -eq 0 -and $major.Count -eq 0 -and -not $bindgenHasUpdate) {
    Write-Host "  ✔ No held-back, major, or bindgen updates available — everything is current." -ForegroundColor Green
    Write-Host ""
    Write-Host "  (Routine semver-compatible updates are applied directly with 'cargo update'.)" -ForegroundColor DarkGray
    exit 0
}

# ── Quarantine + publish dates for COMPATIBLE held-back updates ───────────────
$now = Get-Date
$results = [System.Collections.Generic.List[object]]::new()

if ($compat.Count -gt 0) {
    Write-Host "Fetching publish dates for $($compat.Count) compatible update(s)…" -ForegroundColor Blue
    Write-Host ""
    foreach ($dep in $compat) {
        $url = "https://crates.io/api/v1/crates/$($dep.Name)/$($dep.Available)"
        try {
            $response = Invoke-RestMethod -Uri $url -Headers $Headers -TimeoutSec 10
            $createdAt = [DateTime]::Parse($response.version.created_at)
            $pubDate = $createdAt.ToString('yyyy-MM-dd')
            $ageDays = [math]::Floor(($now - $createdAt).TotalDays)
        }
        catch {
            $pubDate = 'unknown'; $ageDays = 999
        }
        $status = if ($ageDays -lt $QuarantineDays) { 'quarantine' } else { 'ok' }
        $results.Add([PSCustomObject]@{
                Name = $dep.Name; Current = $dep.Current; Available = $dep.Available
                Published = $pubDate; AgeDays = $ageDays; Status = $status
            })
        Start-Sleep -Seconds 1  # crates.io: max ~1 req/sec
    }
}

$safeCount = ($results | Where-Object Status -eq 'ok').Count
$quarantineCount = ($results | Where-Object Status -eq 'quarantine').Count

# ── Report: compatible held-back updates ──────────────────────────────────────
if ($results.Count -gt 0) {
    # Inner width must match the column rule below:
    # (20+2)+(13+2)+(13+2)+(12+2)+(7+2)+(11+2) + 5 column joints = 93.
    $inner = 93
    $tl = '  Held-back COMPATIBLE updates'
    $tr = "quarantine: ${QuarantineDays}d  "
    $sp = $inner - $tl.Length - $tr.Length; if ($sp -lt 1) { $sp = 1 }
    Write-Host ('┌' + ('─' * $inner) + '┐') -ForegroundColor White
    Write-Host ('│' + $tl + (' ' * $sp) + $tr + '│') -ForegroundColor White
    Write-Host "├──────────────────────┬───────────────┬───────────────┬──────────────┬─────────┬─────────────┤" -ForegroundColor White
    $header = '│ {0,-20} │ {1,-13} │ {2,-13} │ {3,-12} │ {4,-7} │ {5,-11} │' -f 'Crate', 'Current', 'Available', 'Published', 'Age', 'Status'
    Write-Host $header -ForegroundColor White
    Write-Host "├──────────────────────┼───────────────┼───────────────┼──────────────┼─────────┼─────────────┤" -ForegroundColor White

    foreach ($r in $results) {
        $ageStr = "$($r.AgeDays)d"
        # Plain ASCII status (glyphs kept to the summary lines, matching the
        # bash script, so column width is never skewed by wide characters).
        if ($r.Status -eq 'quarantine') { $statusStr = 'blocked'; $ageColor = 'Red'; $statusColor = 'Red' }
        else { $statusStr = 'safe'; $ageColor = 'Green'; $statusColor = 'Green' }

        Write-Host -NoNewline '│ '
        Write-Host -NoNewline ('{0,-20}' -f $r.Name) -ForegroundColor Cyan
        Write-Host -NoNewline ' │ '
        Write-Host -NoNewline ('{0,-13}' -f $r.Current) -ForegroundColor DarkGray
        Write-Host -NoNewline ' │ '
        Write-Host -NoNewline ('{0,-13}' -f $r.Available) -ForegroundColor Yellow
        Write-Host -NoNewline ' │ '
        Write-Host -NoNewline ('{0,-12}' -f $r.Published)
        Write-Host -NoNewline ' │ '
        Write-Host -NoNewline ('{0,-7}' -f $ageStr) -ForegroundColor $ageColor
        Write-Host -NoNewline ' │ '
        Write-Host -NoNewline ('{0,-11}' -f $statusStr) -ForegroundColor $statusColor
        Write-Host ' │'
    }
    Write-Host "└──────────────────────┴───────────────┴───────────────┴──────────────┴─────────┴─────────────┘" -ForegroundColor White
    Write-Host ""
    Write-Host "  ✔ $safeCount compatible update(s) safe to apply (≥ $QuarantineDays days old)" -ForegroundColor Green
    Write-Host "  ✖ $quarantineCount compatible update(s) blocked by quarantine (< $QuarantineDays days old)" -ForegroundColor Red
    Write-Host ""
}

# ── Report: MAJOR (incompatible) updates — manual only ────────────────────────
if ($major.Count -gt 0) {
    Write-Host "⚠ MAJOR updates available ($($major.Count)) — not applied automatically" -ForegroundColor Yellow
    foreach ($m in $major) {
        Write-Host -NoNewline "  "
        Write-Host -NoNewline $m.Name -ForegroundColor Cyan
        Write-Host -NoNewline "  $($m.Current) → " -ForegroundColor DarkGray
        Write-Host $m.Available -ForegroundColor Yellow
    }
    Write-Host ""
    Write-Host '  These cross a semver-incompatible boundary. To take one, bump its version' -ForegroundColor DarkGray
    Write-Host '  requirement in the relevant Cargo.toml (e.g. foo = "54"), then run cargo build' -ForegroundColor DarkGray
    Write-Host '  and run the test suite — APIs may have changed. For the Arrow/Parquet and' -ForegroundColor DarkGray
    Write-Host '  DataFusion crates, bump the whole set together (see CLAUDE.md).' -ForegroundColor DarkGray
    Write-Host ""
}

# ── Report: bindgen advisory ──────────────────────────────────────────────────
if ($bindgenHasUpdate) {
    Write-Host "⚠ bindgen update available: $bindgenPin → $bindgenLatest" -ForegroundColor Yellow
    Write-Host '  Do NOT bump bindgen casually.' -ForegroundColor Red
    Write-Host ('  It is exact-pinned (bindgen = "=' + $bindgenPin + '") in the workspace Cargo.toml because')
    Write-Host '  its generated output drives the checked-in per-target FFI bindings in:'
    Write-Host '    crates/readstat-sys/src/bindings/' -ForegroundColor Cyan
    Write-Host '    crates/readstat-iconv-sys/src/bindings/' -ForegroundColor Cyan
    Write-Host '  A bindgen bump can silently change that output, so it must be paired with'
    Write-Host '  regenerating every target''s bindings. In short:'
    Write-Host ''
    Write-Host ('    Locally  — bump the pin to "=' + $bindgenLatest + '" in Cargo.toml, then regenerate')
    Write-Host '              your host target and verify it works (needs libclang):'
    Write-Host '                cargo build -p readstat-sys --features buildtime_bindgen'
    Write-Host '                cargo test --workspace'
    Write-Host '              (Windows also: cargo build -p readstat-iconv-sys --features buildtime_bindgen)'
    Write-Host "    In CI    — push; the 'readstat-sys cross-platform CI' regen/regen-iconv jobs"
    Write-Host '              regenerate the other targets. Their drift check fails on purpose for'
    Write-Host '              each stale file; download the uploaded artifacts, commit them, re-push.'
    Write-Host ''
    Write-Host '  Full step-by-step: docs/CI-CD.md -> "Updating bindgen ... regenerating bindings"'
    Write-Host "  (-Apply will NOT touch bindgen; the exact pin also blocks 'cargo update'.)" -ForegroundColor DarkGray
    Write-Host ''
}

if ($quarantineCount -gt 0) {
    Write-Host "  ⚠ Quarantined updates were published too recently." -ForegroundColor Yellow
    Write-Host "  Wait until they are at least $QuarantineDays days old before upgrading."
    Write-Host "  This buffer allows security scanners (cargo-audit, cargo-deny, RustSec)"
    Write-Host "  to flag any malicious or compromised releases."
    Write-Host ""
}

# ── Apply mode (compatible + safe only) ───────────────────────────────────────
if ($Apply) {
    if ($results.Count -eq 0 -or $safeCount -eq 0) {
        Write-Host "Nothing to apply — no compatible updates cleared quarantine." -ForegroundColor DarkGray
    }
    else {
        Write-Host "Applying $safeCount safe compatible update(s) via cargo update…" -ForegroundColor Blue
        Write-Host ""
        $applied = 0; $skipped = 0
        foreach ($r in $results) {
            if ($r.Status -eq 'quarantine') {
                Write-Host -NoNewline "  ✖ "
                Write-Host -NoNewline "Skipping " -ForegroundColor DarkGray
                Write-Host -NoNewline $r.Name -ForegroundColor Cyan
                Write-Host " (quarantined)" -ForegroundColor DarkGray
                $skipped++
                continue
            }
            Write-Host -NoNewline "  ↻ Updating "
            Write-Host -NoNewline $r.Name -ForegroundColor Cyan
            Write-Host -NoNewline " → "
            Write-Host -NoNewline $r.Available -ForegroundColor Yellow
            Write-Host -NoNewline "…"

            $ok = $false
            cargo update -p $r.Name --precise $r.Available 2>&1 | Out-Null
            if ($LASTEXITCODE -eq 0) { $ok = $true }
            else { cargo update -p $r.Name 2>&1 | Out-Null; if ($LASTEXITCODE -eq 0) { $ok = $true } }

            if ($ok) { Write-Host " ✔" -ForegroundColor Green; $applied++ }
            else { Write-Host " ✖ held back (likely a transitive constraint)" -ForegroundColor Red }
        }
        Write-Host ""
        Write-Host "Apply complete" -ForegroundColor White
        Write-Host "  ✔ $applied crate(s) updated in Cargo.lock" -ForegroundColor Green
        if ($skipped -gt 0) {
            Write-Host "  ✖ $skipped crate(s) skipped (quarantined)" -ForegroundColor Red
        }
        Write-Host ""
        Write-Host "Note: Only Cargo.lock was updated (semver-compatible range)." -ForegroundColor DarkGray
        Write-Host "MAJOR bumps and bindgen require the manual steps described above." -ForegroundColor DarkGray
    }
}
else {
    Write-Host "Run with -Apply to pull safe compatible updates into Cargo.lock." -ForegroundColor DarkGray
}

# Recommend complementary tools
Write-Host ""
Write-Host "Tip: Pair this with 'cargo audit' and 'cargo deny check' for full supply chain coverage." -ForegroundColor DarkGray
Write-Host "Tip: 'cargo update' applies all routine semver-compatible updates at once." -ForegroundColor DarkGray
