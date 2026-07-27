#!/usr/bin/env bash
# Validate and optionally publish the canonical synthetic SAS benchmark corpus.
#
# Usage:
#   ./scripts/publish-benchmark.sh             # validate and preview
#   ./scripts/publish-benchmark.sh --publish   # validate, tag, and publish

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

TAG="benchmark-data-v1"
BENCHMARK_DIR="$ROOT_DIR/benchmark-data"
DATASET="${BENCHMARK_DATASET:-$BENCHMARK_DIR/readstat_benchmark_v1.sas7bdat}"
MANIFEST="${BENCHMARK_MANIFEST:-$BENCHMARK_DIR/readstat_benchmark_v1_manifest.txt}"
EXPECTED_ROWS=4000000
MAX_ASSET_BYTES=$((2 * 1024 * 1024 * 1024))
PUBLISH=false

case "${1:-}" in
    "") ;;
    --publish) PUBLISH=true ;;
    -h|--help)
        sed -n '2,6p' "$0"
        exit 0
        ;;
    *)
        echo "Usage: $0 [--publish]" >&2
        exit 2
        ;;
esac

for command in cargo gh git wc; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "error: required command not found: $command" >&2
        exit 1
    fi
done

if command -v sha256sum >/dev/null 2>&1; then
    sha256_file() {
        sha256sum "$1" | awk '{print $1}'
    }
elif command -v shasum >/dev/null 2>&1; then
    sha256_file() {
        shasum -a 256 "$1" | awk '{print $1}'
    }
else
    echo "error: sha256sum (Linux) or shasum (macOS) is required" >&2
    exit 1
fi

if [[ ! -s "$DATASET" ]]; then
    echo "error: dataset not found or empty: $DATASET" >&2
    echo "Copy it to: $BENCHMARK_DIR/readstat_benchmark_v1.sas7bdat" >&2
    exit 1
fi
if [[ ! -s "$MANIFEST" ]]; then
    echo "error: manifest not found or empty: $MANIFEST" >&2
    echo "Copy it to: $BENCHMARK_DIR/readstat_benchmark_v1_manifest.txt" >&2
    exit 1
fi

dataset_name="$(basename "$DATASET")"
if [[ "$dataset_name" != "readstat_benchmark_v1.sas7bdat" ]]; then
    echo "error: canonical dataset must be named readstat_benchmark_v1.sas7bdat" >&2
    exit 1
fi

dataset_bytes="$(wc -c <"$DATASET")"
if ((dataset_bytes >= MAX_ASSET_BYTES)); then
    echo "error: dataset is $dataset_bytes bytes; GitHub release assets must be under 2 GiB" >&2
    exit 1
fi

for required_text in \
    "Dataset: data.readstat_benchmark_v1" \
    "Rows: $EXPECTED_ROWS" \
    "SAS version:" \
    "Operating system" \
    "Session encoding:" \
    "PROC CONTENTS"; do
    if ! grep -Fq "$required_text" "$MANIFEST"; then
        echo "error: manifest is missing required text: $required_text" >&2
        exit 1
    fi
done

cd "$ROOT_DIR"

if [[ -n "$(git status --short)" ]]; then
    echo "error: repository working tree is not clean" >&2
    exit 1
fi

commit="$(git rev-parse HEAD)"
remote_main="$(git ls-remote origin refs/heads/main | awk '{print $1}')"
if [[ -z "$remote_main" || "$commit" != "$remote_main" ]]; then
    echo "error: HEAD ($commit) does not match origin/main (${remote_main:-unavailable})" >&2
    exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
    echo "error: GitHub CLI is not authenticated; run: gh auth login" >&2
    exit 1
fi

if gh release view "$TAG" >/dev/null 2>&1; then
    echo "error: release $TAG already exists; benchmark releases are immutable" >&2
    exit 1
fi
if [[ -n "$(git ls-remote --tags origin "refs/tags/$TAG")" ]]; then
    echo "error: remote tag $TAG already exists without a release" >&2
    exit 1
fi

echo "Validating the canonical dataset with the one-pass reader..."
validation="$(cargo run --quiet --release \
    -p readstat \
    --example large_sas_benchmark \
    --no-default-features -- \
    "$DATASET" \
    --chunk-rows 10000 \
    --mode one-pass)"
printf '%s\n' "$validation"

if ! grep -qx "rows: $EXPECTED_ROWS" <<<"$validation"; then
    echo "error: reader did not report exactly $EXPECTED_ROWS rows" >&2
    exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
checksum_file="$tmp_dir/readstat_benchmark_v1.sas7bdat.sha256"
notes_file="$tmp_dir/release-notes.md"
digest="$(sha256_file "$DATASET")"
printf '%s  %s\n' "$digest" "$dataset_name" >"$checksum_file"

manifest_digest="$(grep -Eio '[0-9a-f]{64}' "$MANIFEST" | head -n1 | tr '[:upper:]' '[:lower:]' || true)"
if [[ -n "$manifest_digest" && "$manifest_digest" != "$digest" ]]; then
    echo "error: manifest SHA-256 does not match the dataset" >&2
    exit 1
fi

cat >"$notes_file" <<EOF
# readstat-rs synthetic benchmark corpus v1

Canonical tall SAS7BDAT workload for reader, writer, memory, and parallelism benchmarks.

- Rows: 4,000,000
- Numeric columns: 12
- Character columns: 8 × 32 bytes
- SAS compression: none
- SHA-256: \`$digest\`
- Generator commit: \`$commit\`

The manifest records the generator parameters, SAS version, operating system,
session encoding, file size, checksum, and complete PROC CONTENTS output.

Verify after downloading:

\`\`\`bash
# Linux
sha256sum -c readstat_benchmark_v1.sas7bdat.sha256

# macOS
shasum -a 256 -c readstat_benchmark_v1.sas7bdat.sha256
\`\`\`
EOF

echo
echo "Ready to create GitHub release:"
echo "  tag:      $TAG"
echo "  commit:   $commit"
echo "  dataset:  $DATASET ($dataset_bytes bytes)"
echo "  manifest: $MANIFEST"
echo "  sha256:   $digest"

if [[ "$PUBLISH" != true ]]; then
    echo
    echo "Validation passed. Publish with: ./scripts/publish-benchmark.sh --publish"
    exit 0
fi

gh release create "$TAG" \
    "$DATASET#Canonical SAS7BDAT benchmark corpus" \
    "$MANIFEST#Generation and environment manifest" \
    "$checksum_file#SHA-256 checksum" \
    --target "$commit" \
    --title "readstat-rs benchmark data v1" \
    --notes-file "$notes_file" \
    --latest=false

echo "Published immutable benchmark release $TAG."
