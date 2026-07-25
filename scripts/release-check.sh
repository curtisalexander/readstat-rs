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
if cargo clippy --workspace --all-targets --all-features -- -D warnings &>/dev/null; then
    pass "cargo clippy"
else
    fail "cargo clippy — warnings or errors found"
fi

# 2b. readstat-wasm (excluded from workspace — check separately)
echo "Checking readstat-wasm..."
WASM_DIR="$ROOT_DIR/crates/readstat-wasm"
if [ -d "$WASM_DIR" ]; then
    if (cd "$WASM_DIR" && cargo fmt -- --check) &>/dev/null; then
        pass "readstat-wasm fmt"
    else
        fail "readstat-wasm fmt — run 'cargo fmt' in crates/readstat-wasm/"
    fi
    if (cd "$WASM_DIR" && cargo clippy --all-targets -- -D warnings) &>/dev/null; then
        pass "readstat-wasm clippy"
    else
        fail "readstat-wasm clippy — warnings or errors found"
    fi
    if command -v emcc &>/dev/null && rustup target list --installed | grep -qx wasm32-unknown-emscripten; then
        if (cd "$WASM_DIR" && cargo build --target wasm32-unknown-emscripten --release) &>/dev/null; then
            pass "readstat-wasm Emscripten build"
        else
            fail "readstat-wasm Emscripten build failed"
        fi
    else
        warn "Emscripten/emcc or wasm32-unknown-emscripten unavailable — skipping actual WASM build"
    fi
else
    warn "readstat-wasm directory not found — skipping"
fi

# 2c. MSRV — the workspace must at least type-check on the exact toolchain
# declared in `[workspace.package] rust-version`. Skipped (with a warning) if
# that toolchain isn't installed; CI's `msrv` job enforces it regardless.
MSRV="$(grep -m1 '^rust-version' "$ROOT_DIR/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/')"
echo "Checking MSRV ($MSRV)..."
if rustup toolchain list 2>/dev/null | grep -q "^$MSRV"; then
    if cargo "+$MSRV" check --workspace --all-targets &>/dev/null; then
        pass "MSRV $MSRV check"
    else
        fail "MSRV $MSRV check — workspace does not build on rust-version; bump rust-version or fix"
    fi
else
    warn "MSRV toolchain $MSRV not installed — skipping (install: rustup toolchain install $MSRV)"
fi

# 3. Tests
echo "Running tests..."
if cargo check --workspace --all-targets --all-features &>/dev/null; then
    pass "cargo check --workspace --all-targets --all-features"
else
    fail "workspace all-feature/all-target check failed"
fi
if cargo check -p readstat --no-default-features &>/dev/null \
    && cargo check -p readstat-cli --no-default-features &>/dev/null; then
    pass "minimal feature builds"
else
    fail "minimal feature builds"
fi
if cargo test --workspace --all-features &>/dev/null; then
    pass "cargo test"
else
    fail "cargo test — some tests failed"
fi

# 4. Doc build
echo "Checking doc build..."
if "$SCRIPT_DIR/check-arrow-lockstep.sh" &>/dev/null; then
    pass "Arrow/DataFusion lockstep"
else
    fail "Arrow/DataFusion lockstep"
fi
if cargo doc --workspace --all-features --no-deps &>/dev/null; then
    pass "cargo doc"
else
    fail "cargo doc — build failed"
fi
if bash "$SCRIPT_DIR/build-book.sh" &>/dev/null; then pass "mdBook"; else fail "mdBook build failed"; fi

echo "Checking advertised examples..."
if cargo check --manifest-path "$ROOT_DIR/examples/api-demo/rust-server/Cargo.toml" &>/dev/null; then pass "Rust API server"; else fail "Rust API server"; fi
if cargo check --manifest-path "$ROOT_DIR/examples/api-demo/python-server/readstat_py/Cargo.toml" &>/dev/null; then pass "PyO3 extension"; else fail "PyO3 extension"; fi

# 5. cargo-deny (optional)
echo "Checking dependencies..."
if command -v cargo-deny &>/dev/null; then
    if cargo deny check &>/dev/null; then
        pass "cargo deny"
    else
        fail "cargo deny — license/security issues found"
    fi
else
    warn "cargo-deny not installed — skipping (install with: cargo install cargo-deny)"
fi

# 6. Version consistency
echo "Checking version consistency..."
READSTAT_VER=$(grep '^version' "$ROOT_DIR/crates/readstat/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/')
CLI_VER=$(grep '^version' "$ROOT_DIR/crates/readstat-cli/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/')
SYS_VER=$(grep '^version' "$ROOT_DIR/crates/readstat-sys/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/')
ICONV_VER=$(grep '^version' "$ROOT_DIR/crates/readstat-iconv-sys/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/')

# readstat and readstat-cli should match
if [ "$READSTAT_VER" = "$CLI_VER" ]; then
    pass "readstat ($READSTAT_VER) and readstat-cli ($CLI_VER) versions match"
else
    fail "Version mismatch: readstat=$READSTAT_VER, readstat-cli=$CLI_VER"
fi

# readstat-sys and readstat-iconv-sys version independently (each is bumped
# only when it changes); the real constraint is that readstat-sys's declared
# dependency requirement matches readstat-iconv-sys's actual version.
SYS_ICONV_DEP=$(grep 'readstat-iconv-sys' "$ROOT_DIR/crates/readstat-sys/Cargo.toml" | grep 'version' | sed 's/.*version = "\(.*\)".*/\1/')
if [ "$SYS_ICONV_DEP" = "$ICONV_VER" ]; then
    pass "readstat-sys depends on readstat-iconv-sys $SYS_ICONV_DEP (matches)"
else
    fail "readstat-sys depends on readstat-iconv-sys $SYS_ICONV_DEP but current is $ICONV_VER"
fi

# Check that readstat depends on the current readstat-sys version
READSTAT_SYS_DEP=$(grep 'readstat-sys' "$ROOT_DIR/crates/readstat/Cargo.toml" | grep 'version' | head -1 | sed 's/.*version = "\(.*\)".*/\1/')
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
    if cargo package -p "$crate" --allow-dirty --list &>/dev/null; then
        pass "cargo package -p $crate --list"
    else
        fail "cargo package -p $crate --list"
    fi
    # Test the exit code directly. Grepping stdout/stderr for a fixed string
    # (e.g. "warning: aborting") fails open — modern cargo doesn't print it, and
    # a pipe would mask cargo's exit status anyway. `cargo package` exits
    # nonzero when packaging actually fails.
    #
    # Exception: before the FIRST publish, packaging any crate with a path
    # dependency fails with "no matching package named `<dep>` found" because
    # the dependency isn't on crates.io yet. That's expected — downgrade it to
    # a warning so the first-publish run isn't littered with false failures.
    # Real packaging errors still fail.
    if pkg_output=$(cargo package -p "$crate" --locked --allow-dirty 2>&1); then
        pass "cargo package -p $crate"
    elif grep -q "no matching package named" <<<"$pkg_output"; then
        warn "cargo package -p $crate — path dependency not on crates.io yet (expected before first publish)"
    else
        fail "cargo package -p $crate"
    fi
done

# 9. Vendor status
echo "Checking vendor status..."
if "$SCRIPT_DIR/vendor.sh" status &>/dev/null; then pass "vendor status"; else fail "vendor status could not be verified"; fi

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
