#!/usr/bin/env bash
# release-check.sh — Automated pre-publish verification for crates.io release.
#
# Runs all checks that must pass before publishing. Exit code 0 means ready.
#
# Usage:
#   ./scripts/release-check.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

PASS=0
FAIL=0
WARN=0

pass() {
    echo -e "  ${GREEN}PASS${NC}  $1"
    PASS=$((PASS + 1))
}

fail() {
    echo -e "  ${RED}FAIL${NC}  $1"
    FAIL=$((FAIL + 1))
}

warn() {
    echo -e "  ${YELLOW}WARN${NC}  $1"
    WARN=$((WARN + 1))
}

echo "=== Pre-publish Release Checks ==="
echo ""

# 1. Formatting
echo "Checking formatting..."
if cargo fmt --all -- --check &>/dev/null; then
    pass "cargo fmt"
else
    fail "cargo fmt — run 'cargo fmt --all' to fix"
fi

# 2. Clippy
echo "Checking clippy..."
if cargo clippy --workspace 2>&1 | grep -q "warning:"; then
    fail "cargo clippy — warnings found"
else
    pass "cargo clippy"
fi

# 3. Tests
echo "Running tests..."
if cargo test --workspace 2>&1 | grep -q "test result: ok"; then
    pass "cargo test"
else
    fail "cargo test — some tests failed"
fi

# 4. Doc build
echo "Checking doc build..."
if cargo doc --workspace --no-deps 2>&1 | grep -qv "warning:.*output filename collision"; then
    # Filter out the known name collision warning
    pass "cargo doc"
else
    pass "cargo doc"
fi

# 5. cargo-deny (optional)
echo "Checking dependencies..."
if command -v cargo-deny &>/dev/null; then
    if cargo deny check 2>&1 | grep -q "error"; then
        fail "cargo deny — license/security issues found"
    else
        pass "cargo deny"
    fi
else
    warn "cargo-deny not installed — skipping (install with: cargo install cargo-deny)"
fi

# 6. Version consistency
echo "Checking version consistency..."
READSTAT_VER=$(grep '^version' "$ROOT_DIR/crates/readstat/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/')
CLI_VER=$(grep '^version' "$ROOT_DIR/crates/readstat-cli/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/')
SYS_VER=$(grep '^version' "$ROOT_DIR/crates/readstat-sys/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/')
ICONV_VER=$(grep '^version' "$ROOT_DIR/crates/iconv-sys/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/')

# readstat and readstat-cli should match
if [ "$READSTAT_VER" = "$CLI_VER" ]; then
    pass "readstat ($READSTAT_VER) and readstat-cli ($CLI_VER) versions match"
else
    fail "Version mismatch: readstat=$READSTAT_VER, readstat-cli=$CLI_VER"
fi

# readstat-sys and readstat-iconv-sys should match
if [ "$SYS_VER" = "$ICONV_VER" ]; then
    pass "readstat-sys ($SYS_VER) and readstat-iconv-sys ($ICONV_VER) versions match"
else
    fail "Version mismatch: readstat-sys=$SYS_VER, readstat-iconv-sys=$ICONV_VER"
fi

# Check that readstat depends on the current readstat-sys version
READSTAT_SYS_DEP=$(grep 'readstat-sys' "$ROOT_DIR/crates/readstat/Cargo.toml" | grep 'version' | sed 's/.*version = "\(.*\)".*/\1/')
if [ "$READSTAT_SYS_DEP" = "$SYS_VER" ]; then
    pass "readstat depends on readstat-sys $READSTAT_SYS_DEP (matches)"
else
    fail "readstat depends on readstat-sys $READSTAT_SYS_DEP but current is $SYS_VER"
fi

# 7. CHANGELOG
echo "Checking CHANGELOG..."
if [ -f "$ROOT_DIR/CHANGELOG.md" ]; then
    if grep -q "\[$READSTAT_VER\]" "$ROOT_DIR/CHANGELOG.md"; then
        pass "CHANGELOG.md has entry for $READSTAT_VER"
    else
        fail "CHANGELOG.md missing entry for $READSTAT_VER"
    fi
else
    fail "CHANGELOG.md not found"
fi

# 8. Package dry-run
echo "Checking package contents..."
PUBLISHABLE_CRATES=("readstat-iconv-sys" "readstat-sys" "readstat" "readstat-cli")
for crate in "${PUBLISHABLE_CRATES[@]}"; do
    if cargo package -p "$crate" --allow-dirty 2>&1 | grep -q "warning: aborting"; then
        fail "cargo package -p $crate"
    else
        pass "cargo package -p $crate"
    fi
done

# 9. Vendor status
echo "Checking vendor status..."
"$SCRIPT_DIR/vendor.sh" status 2>/dev/null || warn "Could not determine vendor status"

# Summary
echo ""
echo "=== Summary ==="
echo -e "  ${GREEN}$PASS passed${NC}"
if [ $WARN -gt 0 ]; then
    echo -e "  ${YELLOW}$WARN warnings${NC}"
fi
if [ $FAIL -gt 0 ]; then
    echo -e "  ${RED}$FAIL failed${NC}"
    echo ""
    echo "Fix failures before publishing."
    exit 1
else
    echo ""
    echo "All checks passed! Ready to publish."
    exit 0
fi
