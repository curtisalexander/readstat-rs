#!/usr/bin/env bash
# Download, unzip, and rename the AHS 2019 National PUF sas7bdat file
# The file is gitignored via the _*.sas7bdat pattern
#
# Run from any directory:
#   ./crates/readstat-tests/util/download_ahs.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../tests/data"
URL="http://www2.census.gov/programs-surveys/ahs/2019/AHS%202019%20National%20PUF%20v1.1%20Flat%20SAS.zip"
ZIP_FILE="$DATA_DIR/ahs2019.zip"
FINAL_FILE="$DATA_DIR/_ahs2019n.sas7bdat"

if [ -f "$FINAL_FILE" ]; then
    echo "File already exists: $FINAL_FILE"
    exit 0
fi

echo "Downloading AHS 2019 National PUF..."
curl -L -o "$ZIP_FILE" "$URL"

echo "Extracting..."
TEMP_DIR="$DATA_DIR/ahs_temp"
mkdir -p "$TEMP_DIR"
unzip -o "$ZIP_FILE" -d "$TEMP_DIR"

# Find the extracted sas7bdat file
EXTRACTED=$(find "$TEMP_DIR" -name "*.sas7bdat" | head -1)

if [ -z "$EXTRACTED" ]; then
    echo "Error: Could not find extracted sas7bdat file"
    rm -f "$ZIP_FILE"
    rm -rf "$TEMP_DIR"
    exit 1
fi

mv "$EXTRACTED" "$FINAL_FILE"
rm -f "$ZIP_FILE"
rm -rf "$TEMP_DIR"

echo "Done: $FINAL_FILE"
