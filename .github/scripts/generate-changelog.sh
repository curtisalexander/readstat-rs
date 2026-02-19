#!/usr/bin/env bash
#
# Generate a changelog from git history between two tags.
#
# Usage:
#   generate-changelog.sh                  # auto-detect current and previous tag
#   generate-changelog.sh v0.17.0          # from previous tag to v0.17.0
#   generate-changelog.sh v0.17.0 v0.13.0  # from v0.13.0 to v0.17.0
#
# Output: Markdown-formatted changelog to stdout.

set -euo pipefail

CURRENT_TAG="${1:-}"
PREVIOUS_TAG="${2:-}"

# Auto-detect current tag from HEAD if not provided
if [ -z "$CURRENT_TAG" ]; then
    CURRENT_TAG=$(git describe --tags --exact-match HEAD 2>/dev/null || echo "")
    if [ -z "$CURRENT_TAG" ]; then
        echo "Error: HEAD is not tagged and no tag was provided." >&2
        echo "Usage: $0 [CURRENT_TAG] [PREVIOUS_TAG]" >&2
        exit 1
    fi
fi

# Auto-detect previous tag if not provided
if [ -z "$PREVIOUS_TAG" ]; then
    PREVIOUS_TAG=$(git tag --sort=-version:refname | grep -v "^${CURRENT_TAG}$" | head -1 || echo "")
fi

# Build the git log range
if [ -n "$PREVIOUS_TAG" ]; then
    RANGE="${PREVIOUS_TAG}..${CURRENT_TAG}"
    echo "## What's Changed in ${CURRENT_TAG}"
    echo ""
    echo "**Full Changelog**: ${PREVIOUS_TAG}...${CURRENT_TAG}"
    echo ""
else
    RANGE="${CURRENT_TAG}"
    echo "## What's Changed in ${CURRENT_TAG}"
    echo ""
fi

# Categorize commits
FEATURES=""
FIXES=""
OTHER=""

while IFS= read -r line; do
    subject=$(echo "$line" | sed 's/^[a-f0-9]* //')
    hash=$(echo "$line" | awk '{print $1}')
    short="${subject} (${hash})"

    # Skip merge commits that slipped through
    if echo "$subject" | grep -qi "^Merge "; then
        continue
    fi

    # Categorize by commit message keywords
    lower=$(echo "$subject" | tr '[:upper:]' '[:lower:]')
    if echo "$lower" | grep -qE "^(add|implement|support|introduce|create|enable)"; then
        FEATURES="${FEATURES}\n- ${short}"
    elif echo "$lower" | grep -qE "^(fix|correct|resolve|patch|address|repair)"; then
        FIXES="${FIXES}\n- ${short}"
    else
        OTHER="${OTHER}\n- ${short}"
    fi
done < <(git log --oneline --no-merges "$RANGE" 2>/dev/null)

if [ -n "$FEATURES" ]; then
    echo "### Added"
    echo -e "$FEATURES"
    echo ""
fi

if [ -n "$FIXES" ]; then
    echo "### Fixed"
    echo -e "$FIXES"
    echo ""
fi

if [ -n "$OTHER" ]; then
    echo "### Changed"
    echo -e "$OTHER"
    echo ""
fi
