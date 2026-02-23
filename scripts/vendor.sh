#!/usr/bin/env bash
# vendor.sh — Switch vendor directories between git submodule (dev) and
# copied files (crates.io publish) modes.
#
# Usage:
#   ./scripts/vendor.sh prepare   # Copy vendor files, deinit submodules
#   ./scripts/vendor.sh restore   # Remove copies, re-init submodules
#   ./scripts/vendor.sh status    # Show current mode

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCK_FILE="$ROOT_DIR/vendor-lock.txt"

# Vendor directories and their submodule paths
READSTAT_VENDOR="$ROOT_DIR/crates/readstat-sys/vendor/ReadStat"
ICONV_VENDOR="$ROOT_DIR/crates/iconv-sys/vendor/libiconv-win-build"

# Files to copy for readstat-sys (matches Cargo.toml include patterns)
# Only source files needed by build.rs — excludes bin/, fuzz/, test/
READSTAT_PATTERNS=(
    "LICENSE"
    "src/*.c"
    "src/*.h"
    "src/sas"
    "src/spss"
    "src/stata"
    "src/txt"
)

# Files to copy for iconv-sys (matches Cargo.toml include patterns)
ICONV_PATTERNS=(
    "include"
    "lib"
    "libcharset"
    "srclib"
    "COPYING"
    "COPYING.LIB"
    "LICENSE.md"
)

check_git_repo() {
    if ! git -C "$ROOT_DIR" rev-parse --is-inside-work-tree &>/dev/null; then
        echo "Error: Not inside a git repository"
        exit 1
    fi
}

record_submodule_hashes() {
    echo "Recording submodule commit hashes..."
    echo "# Submodule commit hashes — recorded by vendor.sh prepare" > "$LOCK_FILE"
    echo "# Use these to restore exact versions with vendor.sh restore" >> "$LOCK_FILE"
    git -C "$ROOT_DIR" submodule status | while read -r line; do
        echo "$line" >> "$LOCK_FILE"
    done
    echo "Wrote $LOCK_FILE"
}

copy_readstat_files() {
    local src="$READSTAT_VENDOR"
    local tmp="$ROOT_DIR/.vendor-tmp-readstat"

    echo "Copying ReadStat vendor files..."
    rm -rf "$tmp"
    mkdir -p "$tmp"

    # Copy LICENSE
    cp "$src/LICENSE" "$tmp/" 2>/dev/null || true

    # Copy src/*.c and src/*.h
    mkdir -p "$tmp/src"
    cp "$src"/src/*.c "$tmp/src/" 2>/dev/null || true
    cp "$src"/src/*.h "$tmp/src/" 2>/dev/null || true

    # Copy subdirectories
    for subdir in sas spss stata txt; do
        if [ -d "$src/src/$subdir" ]; then
            cp -r "$src/src/$subdir" "$tmp/src/"
        fi
    done

    echo "  Copied $(find "$tmp" -type f | wc -l) files"
    echo "$tmp"
}

copy_iconv_files() {
    local src="$ICONV_VENDOR"
    local tmp="$ROOT_DIR/.vendor-tmp-iconv"

    echo "Copying libiconv vendor files..."
    rm -rf "$tmp"
    mkdir -p "$tmp"

    # Copy directories
    for dir in include lib libcharset srclib; do
        if [ -d "$src/$dir" ]; then
            cp -r "$src/$dir" "$tmp/"
        fi
    done

    # Copy license files
    cp "$src"/COPYING* "$tmp/" 2>/dev/null || true
    cp "$src/LICENSE.md" "$tmp/" 2>/dev/null || true

    echo "  Copied $(find "$tmp" -type f | wc -l) files"
    echo "$tmp"
}

do_prepare() {
    check_git_repo
    echo "=== Preparing vendor directories for crates.io publish ==="

    # Ensure submodules are initialized
    if [ ! -f "$READSTAT_VENDOR/LICENSE" ]; then
        echo "Initializing submodules..."
        git -C "$ROOT_DIR" submodule update --init --recursive
    fi

    # Record commit hashes for reproducibility
    record_submodule_hashes

    # Copy needed files to temp directories
    local readstat_tmp
    readstat_tmp=$(copy_readstat_files)
    local iconv_tmp
    iconv_tmp=$(copy_iconv_files)

    # Deinit submodules (removes the checkout but keeps .gitmodules)
    echo "Deinitializing submodules..."
    git -C "$ROOT_DIR" submodule deinit --force crates/readstat-sys/vendor/ReadStat 2>/dev/null || true
    git -C "$ROOT_DIR" submodule deinit --force crates/iconv-sys/vendor/libiconv-win-build 2>/dev/null || true

    # Remove submodule directories
    rm -rf "$READSTAT_VENDOR"
    rm -rf "$ICONV_VENDOR"

    # Move copied files into place
    mv "$readstat_tmp" "$READSTAT_VENDOR"
    mv "$iconv_tmp" "$ICONV_VENDOR"

    echo ""
    echo "=== Vendor directories prepared for publishing ==="
    echo "ReadStat: $(find "$READSTAT_VENDOR" -type f | wc -l) files"
    echo "libiconv: $(find "$ICONV_VENDOR" -type f | wc -l) files"
    echo ""
    echo "Verify with:"
    echo "  cargo package --list -p readstat-sys --allow-dirty"
    echo "  cargo package --list -p readstat-iconv-sys --allow-dirty"
    echo ""
    echo "To restore submodules after publishing: ./scripts/vendor.sh restore"
}

do_restore() {
    check_git_repo
    echo "=== Restoring git submodule development mode ==="

    # Remove copied vendor directories
    echo "Removing copied vendor files..."
    rm -rf "$READSTAT_VENDOR"
    rm -rf "$ICONV_VENDOR"

    # Re-initialize submodules
    echo "Re-initializing submodules..."
    git -C "$ROOT_DIR" submodule update --init --recursive

    # Verify against lock file if it exists
    if [ -f "$LOCK_FILE" ]; then
        echo ""
        echo "Recorded hashes from last prepare:"
        grep -v "^#" "$LOCK_FILE" || true
        echo ""
        echo "Current submodule status:"
        git -C "$ROOT_DIR" submodule status
    fi

    echo ""
    echo "=== Submodules restored ==="
}

do_status() {
    check_git_repo
    echo "=== Vendor directory status ==="

    # Check ReadStat
    if [ -d "$READSTAT_VENDOR/.git" ] || [ -f "$READSTAT_VENDOR/.git" ]; then
        echo "ReadStat: git submodule (development mode)"
    elif [ -d "$READSTAT_VENDOR" ]; then
        echo "ReadStat: copied files (publish mode)"
        echo "  Files: $(find "$READSTAT_VENDOR" -type f | wc -l)"
    else
        echo "ReadStat: NOT PRESENT"
    fi

    # Check libiconv
    if [ -d "$ICONV_VENDOR/.git" ] || [ -f "$ICONV_VENDOR/.git" ]; then
        echo "libiconv: git submodule (development mode)"
    elif [ -d "$ICONV_VENDOR" ]; then
        echo "libiconv: copied files (publish mode)"
        echo "  Files: $(find "$ICONV_VENDOR" -type f | wc -l)"
    else
        echo "libiconv: NOT PRESENT"
    fi

    # Show lock file if present
    if [ -f "$LOCK_FILE" ]; then
        echo ""
        echo "Lock file: $LOCK_FILE"
        grep -v "^#" "$LOCK_FILE" 2>/dev/null || true
    fi
}

case "${1:-}" in
    prepare)
        do_prepare
        ;;
    restore)
        do_restore
        ;;
    status)
        do_status
        ;;
    *)
        echo "Usage: $0 {prepare|restore|status}"
        echo ""
        echo "  prepare  Copy vendor files for crates.io publishing"
        echo "  restore  Re-initialize git submodules for development"
        echo "  status   Show current vendor directory mode"
        exit 1
        ;;
esac
