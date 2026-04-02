#Requires -Version 7.0
<#
.SYNOPSIS
    Report outdated workspace dependencies with publish dates.

.DESCRIPTION
    Flags recently published crate versions (< QuarantineDays old) as risky
    to help prevent supply chain attacks. Uses cargo update --dry-run and
    the crates.io API.

.PARAMETER QuarantineDays
    Minimum age in days a crate version must have before it is considered
    safe to adopt. Default: 7.

.EXAMPLE
    ./scripts/check-updates.ps1
    ./scripts/check-updates.ps1 -QuarantineDays 3
#>
param(
    [int]$QuarantineDays = 7
)

$ErrorActionPreference = 'Stop'

# ── Gather outdated deps from cargo ──────────────────────────────────────────
Write-Host "`nChecking for outdated dependencies…" -ForegroundColor Blue
Write-Host ""

$raw = cargo update --dry-run --verbose 2>&1 | Out-String
$lines = $raw -split "`n" | Where-Object { $_ -match '^\s*Unchanged' }

if (-not $lines -or $lines.Count -eq 0) {
    Write-Host "  ✔ All dependencies are at their latest compatible versions." -ForegroundColor Green
    exit 0
}

# Parse lines into objects
$deps = foreach ($line in $lines) {
    if ($line -match 'Unchanged\s+(\S+)\s+v(\S+)\s+\(available:\s+v([^)]+)\)') {
        [PSCustomObject]@{
            Name      = $Matches[1]
            Current   = $Matches[2]
            Available = $Matches[3]
        }
    }
}

$count = $deps.Count
Write-Host "Fetching publish dates for $count crate(s) from crates.io…" -ForegroundColor Blue
Write-Host ""

# ── Fetch publish dates from crates.io ───────────────────────────────────────
$now = Get-Date
$results = @()

foreach ($dep in $deps) {
    $url = "https://crates.io/api/v1/crates/$($dep.Name)/$($dep.Available)"
    try {
        $headers = @{ 'User-Agent' = 'readstat-rs-check-updates (https://github.com/curtisalexander/readstat-rs)' }
        $response = Invoke-RestMethod -Uri $url -Headers $headers -TimeoutSec 10
        $createdAt = [DateTime]::Parse($response.version.created_at)
        $pubDate = $createdAt.ToString('yyyy-MM-dd')
        $ageDays = [math]::Floor(($now - $createdAt).TotalDays)
    }
    catch {
        $pubDate = 'unknown'
        $ageDays = 999
    }

    $status = if ($ageDays -lt $QuarantineDays) { 'quarantine' } else { 'ok' }

    $results += [PSCustomObject]@{
        Name      = $dep.Name
        Current   = $dep.Current
        Available = $dep.Available
        Published = $pubDate
        AgeDays   = $ageDays
        Status    = $status
    }

    # Rate-limit: crates.io asks for max 1 req/sec
    Start-Sleep -Seconds 1
}

# ── Print report ─────────────────────────────────────────────────────────────

$safeCount = ($results | Where-Object Status -eq 'ok').Count
$quarantineCount = ($results | Where-Object Status -eq 'quarantine').Count

# Header
$border = '─' * 92
Write-Host "┌${border}┐" -ForegroundColor White
Write-Host ("│  Outdated Dependencies Report" + (' ' * 46) + "quarantine: ${QuarantineDays}d  │") -ForegroundColor White
Write-Host "├──────────────────────┬───────────────┬───────────────┬──────────────┬─────────┬─────────────┤" -ForegroundColor White
$header = '│ {0,-20} │ {1,-13} │ {2,-13} │ {3,-12} │ {4,-7} │ {5,-11} │' -f 'Crate', 'Current', 'Available', 'Published', 'Age', 'Status'
Write-Host $header -ForegroundColor White
Write-Host "├──────────────────────┼───────────────┼───────────────┼──────────────┼─────────┼─────────────┤" -ForegroundColor White

foreach ($r in $results) {
    $ageStr = "$($r.AgeDays)d"
    if ($r.Status -eq 'quarantine') {
        $statusStr = '✖ blocked'
        $ageColor = 'Red'
        $statusColor = 'Red'
    }
    else {
        $statusStr = '✔ safe'
        $ageColor = 'Green'
        $statusColor = 'Green'
    }

    # Build the line piece by piece for coloring
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

# Summary
Write-Host ""
Write-Host "Summary" -ForegroundColor White
Write-Host "  ✔ $safeCount update(s) safe to apply (published ≥ $QuarantineDays days ago)" -ForegroundColor Green
Write-Host "  ✖ $quarantineCount update(s) blocked by quarantine (published < $QuarantineDays days ago)" -ForegroundColor Red
Write-Host ""

if ($quarantineCount -gt 0) {
    Write-Host "  ⚠ Quarantined updates were published too recently." -ForegroundColor Yellow
    Write-Host "  Wait until they are at least $QuarantineDays days old before upgrading."
    Write-Host "  This buffer allows security scanners (cargo-audit, cargo-deny, RustSec)"
    Write-Host "  to flag any malicious or compromised releases."
    Write-Host ""
}

Write-Host "Tip: Pair this with 'cargo audit' and 'cargo deny check' for full supply chain coverage." -ForegroundColor DarkGray
