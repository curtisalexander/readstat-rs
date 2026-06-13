#Requires -Version 7.0
<#
.SYNOPSIS
    Report upstream updates to the vendored git submodules WITHOUT altering them.

.DESCRIPTION
    Read-only: uses `git ls-remote` to query each submodule's upstream, which
    contacts the remote and prints refs but writes NOTHING locally — no fetch,
    no pull, no checkout. The pinned commit recorded by the superproject is
    never touched, so `git status` stays clean.

    For each submodule (from .gitmodules) it reports:
      • the currently pinned commit + nearest tag,
      • whether the upstream default branch has moved past the pin, and
      • whether a newer release tag exists upstream.

    An exact "commits behind" count would require fetching objects (which this
    script deliberately does not do), so the default-branch comparison is
    reported as same / moved rather than a number.

.EXAMPLE
    ./scripts/check-vendor-updates.ps1
#>
param()

$ErrorActionPreference = 'Stop'

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $RepoRoot

function Convert-VerParts([string]$v) {
    $core = (($v -replace '^v', '') -split '[-+]')[0]
    $p = $core -split '\.'
    $maj = if ($p.Count -ge 1 -and $p[0] -match '^\d+$') { [int]$p[0] } else { 0 }
    $min = if ($p.Count -ge 2 -and $p[1] -match '^\d+$') { [int]$p[1] } else { 0 }
    $pat = if ($p.Count -ge 3 -and $p[2] -match '^\d+$') { [int]$p[2] } else { 0 }
    return , @($maj, $min, $pat)
}

# True if $a is strictly newer than $b (leading 'v' and pre-release/build ignored).
function Test-VersionGreater([string]$a, [string]$b) {
    $pa = Convert-VerParts $a; $pb = Convert-VerParts $b
    for ($i = 0; $i -lt 3; $i++) {
        if ($pa[$i] -gt $pb[$i]) { return $true }
        if ($pa[$i] -lt $pb[$i]) { return $false }
    }
    return $false
}

if (-not (Test-Path '.gitmodules')) {
    Write-Host '✖ No .gitmodules found at repo root.' -ForegroundColor Red
    exit 1
}

Write-Host "`nChecking vendored submodules for upstream updates (read-only)…" -ForegroundColor Blue
Write-Host ""

$names = git config -f .gitmodules --name-only --get-regexp '\.path$' |
    ForEach-Object { $_ -replace '\.path$', '' }

$anyUpdate = $false

foreach ($key in $names) {
    if (-not $key) { continue }
    $path = git config -f .gitmodules --get "$key.path"
    $url = git config -f .gitmodules --get "$key.url"
    $shortName = Split-Path $path -Leaf

    Write-Host -NoNewline $shortName -ForegroundColor Cyan
    Write-Host " ($path)" -ForegroundColor DarkGray
    Write-Host "  url:            $url" -ForegroundColor DarkGray

    if (-not (Test-Path (Join-Path $path '.git'))) {
        Write-Host "  ⚠ submodule not initialized — run: git submodule update --init '$path'" -ForegroundColor Yellow
        Write-Host ""
        continue
    }

    $curSha = (git -C $path rev-parse HEAD 2>$null)
    $curDescribe = (git -C $path describe --tags --always 2>$null); if (-not $curDescribe) { $curDescribe = '?' }
    $curTag = (git -C $path describe --tags --abbrev=0 2>$null)
    $curShort = if ($curSha) { $curSha.Substring(0, [Math]::Min(9, $curSha.Length)) } else { '?' }
    Write-Host "  pinned commit:  $curShort  ($curDescribe)" -ForegroundColor DarkGray

    # --- upstream default branch tip (read-only) ---
    $symref = git ls-remote --symref $url HEAD 2>$null
    $remoteHead = $null; $defBranch = 'default'
    foreach ($l in @($symref)) {
        if ($l -match '^ref:\s+refs/heads/(\S+)\s+HEAD') { $defBranch = $Matches[1] }
        elseif ($l -match '^([0-9a-f]{7,40})\s+HEAD') { $remoteHead = $Matches[1] }
    }

    if (-not $remoteHead) {
        Write-Host "  ✖ could not reach upstream (network/remote error)" -ForegroundColor Red
        Write-Host ""
        continue
    }

    $remoteShort = $remoteHead.Substring(0, [Math]::Min(9, $remoteHead.Length))
    if ($remoteHead -eq $curSha) {
        Write-Host -NoNewline "  upstream ${defBranch}:  $remoteShort  " -ForegroundColor DarkGray
        Write-Host "✔ pin is at the branch tip" -ForegroundColor Green
    }
    else {
        Write-Host -NoNewline "  upstream ${defBranch}:  $remoteShort  " -ForegroundColor DarkGray
        Write-Host "⚠ branch has moved (newer commits upstream; fetch to see them)" -ForegroundColor Yellow
        $anyUpdate = $true
    }

    # --- latest upstream release tag (read-only) ---
    $tags = git ls-remote --tags --refs $url 2>$null |
        ForEach-Object { ($_ -split '\s+')[1] -replace 'refs/tags/', '' } |
        Where-Object { $_ -match '^v?[0-9]' }
    $latestTag = $null
    foreach ($t in $tags) { if (-not $latestTag -or (Test-VersionGreater $t $latestTag)) { $latestTag = $t } }

    if (-not $latestTag) {
        Write-Host "  latest tag:     (no version tags upstream)" -ForegroundColor DarkGray
    }
    elseif (-not $curTag) {
        Write-Host -NoNewline "  latest tag:     " -ForegroundColor DarkGray
        Write-Host "$latestTag  (pin has no nearest tag)" -ForegroundColor Yellow
    }
    elseif (Test-VersionGreater $latestTag $curTag) {
        Write-Host -NoNewline "  latest tag:     " -ForegroundColor DarkGray
        Write-Host "$latestTag  ⚠ newer release than pinned $curTag" -ForegroundColor Yellow
        $anyUpdate = $true
    }
    else {
        Write-Host -NoNewline "  latest tag:     " -ForegroundColor DarkGray
        Write-Host "$latestTag  ✔ pin is at or past the latest tag" -ForegroundColor Green
    }
    Write-Host ""
}

if ($anyUpdate) {
    Write-Host "⚠ Upstream changes are available for one or more submodules." -ForegroundColor Yellow
    Write-Host "  To adopt one (this DOES alter the vendored checkout):" -ForegroundColor DarkGray
    Write-Host "    git -C <path> fetch origin" -ForegroundColor DarkGray
    Write-Host "    git -C <path> checkout <commit-or-tag>   # then commit the submodule bump" -ForegroundColor DarkGray
    Write-Host "  Bumping the vendored C also requires regenerating + committing the per-target" -ForegroundColor DarkGray
    Write-Host "  bindings (cargo build -p readstat-sys --features buildtime_bindgen, etc.) —" -ForegroundColor DarkGray
    Write-Host "  CI's drift check enforces this. See docs/RELEASING.md / CHANGELOG.md." -ForegroundColor DarkGray
}
else {
    Write-Host "✔ All vendored submodules are at or ahead of upstream's latest tag and branch tip." -ForegroundColor Green
}
