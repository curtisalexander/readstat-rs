#!/usr/bin/env bash
#
# Test the readstat API server endpoints.
# Usage: bash test_api.sh <base_url> <sas_file>
# Example: bash test_api.sh http://localhost:3000 ../test-data/cars.sas7bdat

set -euo pipefail

BASE_URL="${1:?Usage: test_api.sh <base_url> <sas_file>}"
SAS_FILE="${2:?Usage: test_api.sh <base_url> <sas_file>}"

echo "=== Testing $BASE_URL with $SAS_FILE ==="
echo

echo "--- GET /health ---"
curl -s "$BASE_URL/health" | head -c 500
echo
echo

echo "--- POST /metadata ---"
curl -s -F "file=@$SAS_FILE" "$BASE_URL/metadata" | head -c 2000
echo
echo

echo "--- POST /preview (5 rows) ---"
curl -s -F "file=@$SAS_FILE" "$BASE_URL/preview?rows=5"
echo

echo "--- POST /data?format=csv (first 500 bytes) ---"
curl -s -F "file=@$SAS_FILE" "$BASE_URL/data?format=csv" | head -c 500
echo
echo

echo "--- POST /data?format=ndjson (first 500 bytes) ---"
curl -s -F "file=@$SAS_FILE" "$BASE_URL/data?format=ndjson" | head -c 500
echo
echo

echo "--- POST /data?format=parquet (size check) ---"
curl -s -o /tmp/test_output.parquet -w "HTTP %{http_code}, %{size_download} bytes" \
    -F "file=@$SAS_FILE" "$BASE_URL/data?format=parquet"
echo
echo

echo "--- POST /data?format=feather (size check) ---"
curl -s -o /tmp/test_output.feather -w "HTTP %{http_code}, %{size_download} bytes" \
    -F "file=@$SAS_FILE" "$BASE_URL/data?format=feather"
echo
echo

echo "=== All tests passed ==="
