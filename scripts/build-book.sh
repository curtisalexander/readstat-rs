#!/usr/bin/env bash
#
# Build the mdBook site for readstat-rs.
#
# This script copies markdown files from their canonical locations (docs/,
# crate READMEs) into book/src/, strips navigation lines that only make
# sense on GitHub, replaces GitHub emoji shortcodes with real Unicode
# emoji, then runs mdbook build.  Optionally builds rustdocs and copies
# them into the output so the book can link to them.
#
# Usage:
#   ./scripts/build-book.sh           # build book only
#   ./scripts/build-book.sh --docs    # build book + rustdocs

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BOOK_SRC="$REPO_ROOT/book/src"

# --------------------------------------------------------------------
# 1. Copy and patch docs
# --------------------------------------------------------------------

# Replace GitHub emoji shortcodes with Unicode emoji
replace_emoji() {
    sed \
        -e 's/:key:/🔑/g' \
        -e 's/:bulb:/💡/g' \
        -e 's/:rocket:/🚀/g' \
        -e 's/:package:/📦/g' \
        -e 's/:gear:/⚙️/g' \
        -e 's/:hammer_and_wrench:/🛠️/g' \
        -e 's/:computer:/💻/g' \
        -e 's/:heavy_check_mark:/✅/g' \
        -e 's/:heavy_exclamation_mark:/❗/g' \
        -e 's/:books:/📚/g' \
        -e 's/:jigsaw:/🧩/g' \
        -e 's/:link:/🔗/g' \
        -e 's/:memo:/📝/g' \
        -e 's/:warning:/⚠️/g'
}

# Strip "[< Back to README](...)" nav lines and replace emoji
patch_doc() {
    sed '/^\[< Back to README\]/d' "$1" | replace_emoji
}

echo "Copying docs into book/src/ ..."

# Introduction from root README (strip the CI badge line, replace emoji)
sed '1{/^\[!\[readstat-rs\]/d;}' "$REPO_ROOT/README.md" | replace_emoji > "$BOOK_SRC/introduction.md"

# docs/ files
patch_doc "$REPO_ROOT/docs/BUILDING.md"     > "$BOOK_SRC/building.md"
patch_doc "$REPO_ROOT/docs/USAGE.md"        > "$BOOK_SRC/usage.md"
patch_doc "$REPO_ROOT/docs/ARCHITECTURE.md" > "$BOOK_SRC/architecture.md"
patch_doc "$REPO_ROOT/docs/TECHNICAL.md"    > "$BOOK_SRC/technical.md"
patch_doc "$REPO_ROOT/docs/TESTING.md"      > "$BOOK_SRC/testing.md"
patch_doc "$REPO_ROOT/docs/BENCHMARKING.md"    > "$BOOK_SRC/benchmarking.md"
patch_doc "$REPO_ROOT/docs/CI-CD.md"           > "$BOOK_SRC/ci-cd.md"
patch_doc "$REPO_ROOT/docs/MEMORY_SAFETY.md"   > "$BOOK_SRC/memory-safety.md"

# Crate READMEs
mkdir -p "$BOOK_SRC/crates"
cp "$REPO_ROOT/crates/readstat/README.md"       "$BOOK_SRC/crates/readstat.md"
cp "$REPO_ROOT/crates/readstat-cli/README.md"   "$BOOK_SRC/crates/readstat-cli.md"
cp "$REPO_ROOT/crates/readstat-sys/README.md"   "$BOOK_SRC/crates/readstat-sys.md"
cp "$REPO_ROOT/crates/iconv-sys/README.md"      "$BOOK_SRC/crates/iconv-sys.md"
cp "$REPO_ROOT/crates/readstat-tests/README.md" "$BOOK_SRC/crates/readstat-tests.md"
cp "$REPO_ROOT/crates/readstat-wasm/README.md"  "$BOOK_SRC/crates/readstat-wasm.md"

# --------------------------------------------------------------------
# 2. Build the book
# --------------------------------------------------------------------

echo "Building mdBook ..."
mdbook build "$REPO_ROOT/book"

# --------------------------------------------------------------------
# 3. Optionally build rustdocs and copy into the book output
# --------------------------------------------------------------------

if [[ "${1:-}" == "--docs" ]]; then
    echo "Building rustdocs ..."
    cargo doc --workspace --no-deps --document-private-items

    BOOK_OUT="$REPO_ROOT/target/book"
    echo "Copying rustdocs into $BOOK_OUT/api/ ..."
    cp -r "$REPO_ROOT/target/doc" "$BOOK_OUT/api"
fi

echo "Done. Output is in target/book/"
