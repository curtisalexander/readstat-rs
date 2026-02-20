# Download, unzip, and rename the AHS 2019 National PUF sas7bdat file
# The file is gitignored via the _*.sas7bdat pattern
#
# Run from any directory:
#   .\crates\readstat-tests\util\download_ahs.ps1

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$DataDir = Join-Path $ScriptDir "..\tests\data"
$Url = "http://www2.census.gov/programs-surveys/ahs/2019/AHS%202019%20National%20PUF%20v1.1%20Flat%20SAS.zip"
$ZipFile = Join-Path $DataDir "ahs2019.zip"
$FinalFile = Join-Path $DataDir "_ahs2019n.sas7bdat"

if (Test-Path $FinalFile) {
    Write-Host "File already exists: $FinalFile"
    exit 0
}

Write-Host "Downloading AHS 2019 National PUF..."
Invoke-WebRequest -Uri $Url -OutFile $ZipFile

Write-Host "Extracting..."
$TempDir = Join-Path $DataDir "ahs_temp"
Expand-Archive -Path $ZipFile -DestinationPath $TempDir -Force

# Find the extracted sas7bdat file
$Extracted = Get-ChildItem -Path $TempDir -Filter "*.sas7bdat" -Recurse | Select-Object -First 1

if (-not $Extracted) {
    Write-Host "Error: Could not find extracted sas7bdat file"
    Remove-Item -Path $ZipFile -Force -ErrorAction SilentlyContinue
    Remove-Item -Path $TempDir -Recurse -Force -ErrorAction SilentlyContinue
    exit 1
}

Move-Item -Path $Extracted.FullName -Destination $FinalFile -Force
Remove-Item -Path $ZipFile -Force -ErrorAction SilentlyContinue
Remove-Item -Path $TempDir -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "Done: $FinalFile"
