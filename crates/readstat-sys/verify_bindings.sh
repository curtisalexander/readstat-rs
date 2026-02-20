#!/usr/bin/env bash
# verify_bindings.sh — Verify that readstat-sys Rust bindings cover the full ReadStat C API.
#
# Usage:
#   cd crates/readstat-sys
#   bash verify_bindings.sh           # check latest bindings
#   bash verify_bindings.sh --rebuild # rebuild first, then check
#
# Requires: grep, sort, comm (standard Unix tools)
#
# This script compares:
#   1. Function declarations in vendor/ReadStat/src/readstat.h (the public C API)
#   2. Function definitions in the generated bindings.rs (what Rust can call)
#   3. C source files in vendor/ vs files listed in build.rs
#
# Exit code: 0 if everything matches, 1 if there are gaps.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HEADER="$SCRIPT_DIR/vendor/ReadStat/src/readstat.h"
BUILD_RS="$SCRIPT_DIR/build.rs"
VENDOR_SRC="$SCRIPT_DIR/vendor/ReadStat/src"

# Colors (disable if not a terminal)
if [ -t 1 ]; then
    GREEN='\033[0;32m'
    RED='\033[0;31m'
    YELLOW='\033[0;33m'
    BOLD='\033[1m'
    NC='\033[0m'
else
    GREEN='' RED='' YELLOW='' BOLD='' NC=''
fi

# Optionally rebuild first
if [[ "${1:-}" == "--rebuild" ]]; then
    echo "Rebuilding readstat-sys..."
    cargo build -p readstat-sys 2>/dev/null
    echo ""
fi

# Find the most recent bindings.rs (largest file = most complete, non-emscripten build)
BINDINGS=""
best_size=0
for f in "$SCRIPT_DIR"/../../target/debug/build/readstat-sys-*/out/bindings.rs; do
    if [ -f "$f" ]; then
        size=$(wc -l < "$f")
        if [ -z "$BINDINGS" ] || [ "$size" -gt "$best_size" ]; then
            BINDINGS="$f"
            best_size=$size
        fi
    fi
done

if [ -z "$BINDINGS" ]; then
    echo -e "${RED}Error: No bindings.rs found. Run 'cargo build -p readstat-sys' first.${NC}"
    exit 1
fi

echo -e "${BOLD}ReadStat-sys Binding Verification${NC}"
echo "=================================="
echo ""
echo "Header:   $HEADER"
echo "Bindings: $BINDINGS"
echo "Build.rs: $BUILD_RS"
echo ""

EXIT_CODE=0

# ─── 1. Functions ───────────────────────────────────────────────────────────

# Extract function names from header (public API)
HEADER_FUNCS=$(grep -oE 'readstat_\w+\(' "$HEADER" | sed 's/($//' | sort -u)
HEADER_COUNT=$(echo "$HEADER_FUNCS" | wc -l | tr -d ' ')

# Extract function names from bindings
BINDING_FUNCS=$(grep -oE 'pub fn readstat_\w+' "$BINDINGS" | sed 's/pub fn //' | sort -u)
BINDING_COUNT=$(echo "$BINDING_FUNCS" | wc -l | tr -d ' ')

# Compare
MISSING_FUNCS=$(comm -23 <(echo "$HEADER_FUNCS") <(echo "$BINDING_FUNCS"))
EXTRA_FUNCS=$(comm -13 <(echo "$HEADER_FUNCS") <(echo "$BINDING_FUNCS"))
MISSING_COUNT=$(echo "$MISSING_FUNCS" | grep -c . || true)
EXTRA_COUNT=$(echo "$EXTRA_FUNCS" | grep -c . || true)

echo -e "${BOLD}1. Functions${NC}"
echo "   Header declares:  $HEADER_COUNT functions"
echo "   Bindings contain: $BINDING_COUNT functions"

if [ "$MISSING_COUNT" -eq 0 ]; then
    echo -e "   ${GREEN}All header functions are bound.${NC}"
else
    echo -e "   ${RED}Missing $MISSING_COUNT functions:${NC}"
    echo "$MISSING_FUNCS" | sed 's/^/     - /'
    EXIT_CODE=1
fi

if [ "$EXTRA_COUNT" -gt 0 ]; then
    echo -e "   ${YELLOW}Extra $EXTRA_COUNT functions in bindings (not in header — likely internal):${NC}"
    echo "$EXTRA_FUNCS" | sed 's/^/     - /'
fi

echo ""

# ─── 2. Types ───────────────────────────────────────────────────────────────

# Enums declared in header (match "typedef enum <name>" declarations)
HEADER_ENUMS=$(grep -oE 'typedef enum readstat_\w+' "$HEADER" | sed 's/typedef enum //' | sort -u)
HEADER_ENUM_COUNT=$(echo "$HEADER_ENUMS" | wc -l | tr -d ' ')

# Type aliases in bindings (bindgen represents C enums as type aliases)
BINDING_TYPES=$(grep -oE 'pub type readstat_\w+' "$BINDINGS" | sed 's/pub type //' | sort -u)
BINDING_TYPE_COUNT=$(echo "$BINDING_TYPES" | wc -l | tr -d ' ')

# Check each header enum has a corresponding type alias
MISSING_TYPES=""
for e in $HEADER_ENUMS; do
    if ! echo "$BINDING_TYPES" | grep -qF "$e"; then
        MISSING_TYPES="${MISSING_TYPES}${e}\n"
    fi
done
MISSING_TYPE_COUNT=$(echo -e "$MISSING_TYPES" | grep -c . || true)

echo -e "${BOLD}2. Types${NC}"
echo "   Header enums:     $HEADER_ENUM_COUNT"
echo "   Binding types:    $BINDING_TYPE_COUNT (includes enums + typedefs + callback types)"

if [ "$MISSING_TYPE_COUNT" -eq 0 ]; then
    echo -e "   ${GREEN}All header enums are represented.${NC}"
else
    echo -e "   ${RED}Missing $MISSING_TYPE_COUNT types:${NC}"
    echo -e "$MISSING_TYPES" | grep . | sed 's/^/     - /'
    EXIT_CODE=1
fi

echo ""

# ─── 3. Source Files ────────────────────────────────────────────────────────

# Library source files in vendor (exclude bin/, fuzz/, test/ — those are tools, not library code)
VENDOR_FILES=$(find "$VENDOR_SRC" -name '*.c' \
    ! -path '*/bin/*' \
    ! -path '*/fuzz/*' \
    ! -path '*/test/*' \
    | sed "s|$VENDOR_SRC/||" | tr '\\' '/' | sort)
VENDOR_COUNT=$(echo "$VENDOR_FILES" | wc -l | tr -d ' ')

# Source files referenced in build.rs
# build.rs uses two patterns:
#   1. src.join("file.c")        → file.c
#   2. sas.join("file.c")        → sas/file.c  (where sas = src.join("sas"))
#   3. spss.join("file.c")       → spss/file.c
#   4. stata.join("file.c")      → stata/file.c
#   5. txt.join("file.c")        → txt/file.c
# Extract and reconstruct full relative paths.
_build_files=""

# Direct src.join() calls — files in the root src/ directory
while IFS= read -r f; do
    _build_files="${_build_files}${f}\n"
done < <(grep -oE 'src\.join\("[^"]+"\)' "$BUILD_RS" | grep -oE '"[^"]+"' | tr -d '"' || true)

# Subdirectory .join() calls
for dir in sas spss stata txt; do
    while IFS= read -r f; do
        _build_files="${_build_files}${dir}/${f}\n"
    done < <(grep -oE "${dir}\.join\(\"[^\"]+\"\)" "$BUILD_RS" | grep -oE '"[^"]+"' | tr -d '"' || true)
done

BUILD_FILES=$(echo -e "$_build_files" | grep . | sort -u)
BUILD_COUNT=$(echo "$BUILD_FILES" | wc -l | tr -d ' ')

MISSING_SOURCES=$(comm -23 <(echo "$VENDOR_FILES") <(echo "$BUILD_FILES"))
MISSING_SRC_COUNT=$(echo "$MISSING_SOURCES" | grep -c . || true)

echo -e "${BOLD}3. C Source Files${NC}"
echo "   Vendor library files: $VENDOR_COUNT"
echo "   build.rs compiles:    $BUILD_COUNT"

if [ "$MISSING_SRC_COUNT" -eq 0 ]; then
    echo -e "   ${GREEN}All vendor library source files are compiled.${NC}"
else
    echo -e "   ${RED}Missing $MISSING_SRC_COUNT source files from build.rs:${NC}"
    echo "$MISSING_SOURCES" | sed 's/^/     - /'
    EXIT_CODE=1
fi

echo ""

# ─── 4. Function Coverage by Category ──────────────────────────────────────

echo -e "${BOLD}4. API Coverage by Category${NC}"
echo ""
printf "   %-35s %5s %5s %s\n" "Category" "Hdr" "Bind" "Status"
printf "   %-35s %5s %5s %s\n" "-----------------------------------" "-----" "-----" "------"

check_category() {
    local label="$1"
    local pattern="$2"
    local hdr_count=$(echo "$HEADER_FUNCS" | grep -cE "$pattern" || true)
    local bind_count=$(echo "$BINDING_FUNCS" | grep -cE "$pattern" || true)
    if [ "$hdr_count" -eq "$bind_count" ]; then
        local status="${GREEN}OK${NC}"
    else
        local status="${RED}MISSING $((hdr_count - bind_count))${NC}"
        EXIT_CODE=1
    fi
    printf "   %-35s %5d %5d " "$label" "$hdr_count" "$bind_count"
    echo -e "$status"
}

check_category "Error handling"          "^readstat_error_"
check_category "Metadata accessors"      "^readstat_get_"
check_category "Value accessors"         "^readstat_(int8|int16|int32|float|double|string)_value$|^readstat_value_|^readstat_type_class$"
check_category "Variable accessors"      "^readstat_variable_get_"
check_category "Parser lifecycle"        "^readstat_parser_|^readstat_io_free$"
check_category "Parser callbacks"        "^readstat_set_(metadata|note|variable|fweight|value|error|progress)_handler$"
check_category "Parser I/O handlers"     "^readstat_set_(open|close|seek|read|update)_handler$|^readstat_set_io_ctx$"
check_category "Parser config"           "^readstat_set_(file_char|handler_char|row_)"
check_category "File parsers (readers)"  "^readstat_parse_"
check_category "Schema parsing"          "^readstat_schema_"
check_category "Writer lifecycle"        "^readstat_writer_(init|free)$|^readstat_set_data_writer$"
check_category "Writer labels"           "^readstat_(add_label|label_)"
check_category "Writer variables"        "^readstat_add_variable$|^readstat_variable_set_|^readstat_variable_add_"
check_category "Writer notes/strings"    "^readstat_(add_note|add_string|get_string)"
check_category "Writer metadata setters" "^readstat_writer_set_"
check_category "Writer begin"            "^readstat_begin_writing_"
check_category "Writer validation"       "^readstat_validate_"
check_category "Writer row insertion"    "^readstat_(begin_row|insert_|end_row|end_writing)$"

echo ""

# ─── Summary ────────────────────────────────────────────────────────────────

if [ "$EXIT_CODE" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}PASS: readstat-sys has full coverage of the ReadStat C API.${NC}"
    echo "  - $HEADER_COUNT/$HEADER_COUNT functions bound"
    echo "  - $HEADER_ENUM_COUNT/$HEADER_ENUM_COUNT enum types represented"
    echo "  - $VENDOR_COUNT/$VENDOR_COUNT library source files compiled"
else
    echo -e "${RED}${BOLD}FAIL: Gaps detected in readstat-sys bindings.${NC}"
    echo "  Review the issues above and update build.rs or wrapper.h."
fi

exit $EXIT_CODE
