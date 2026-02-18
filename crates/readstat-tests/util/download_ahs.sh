#!/usr/bin/env bash
# Download, unzip, and rename the AHS 2019 National PUF sas7bdat file
# The file is gitignored via the _*.sas7bdat pattern
#
# Run from the util/ directory:
#   ./download_ahs.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../tests/data"
URL="http://www2.census.gov/programs-surveys/ahs/2019/AHS%202019%20National%20PUF%20v1.1%20Flat%20SAS.zip"
ZIP_FILE="$SCRIPT_DIR/ahs2019.zip"
FINAL_FILE="$DATA_DIR/_ahs2019n.sas7bdat"

if [ -f "$FINAL_FILE" ]; then
    echo "File already exists: $FINAL_FILE"
    exit 0
fi

echo "Downloading AHS 2019 National PUF..."
curl -L -o "$ZIP_FILE" "$URL"

echo "Extracting..."
unzip -o -j "$ZIP_FILE" "*.sas7bdat" -d "$SCRIPT_DIR"

# Find the extracted sas7bdat and rename
EXTRACTED=$(find "$SCRIPT_DIR" -maxdepth 1 -name "ahs2019n.sas7bdat" -o -name "AHS*.[Ss][Aa][Ss]7[Bb][Dd][Aa][Tt]" | head -1)

if [ -z "$EXTRACTED" ]; then
    echo "Error: Could not find extracted sas7bdat file"
    rm -f "$ZIP_FILE"
    exit 1
fi

mv "$EXTRACTED" "$FINAL_FILE"
rm -f "$ZIP_FILE"

echo "Done: $FINAL_FILE"
