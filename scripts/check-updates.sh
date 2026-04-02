#!/usr/bin/env bash
#
# check-updates.sh — Report outdated workspace dependencies with publish dates.
#
# Flags recently published crate versions (< QUARANTINE_DAYS old) as risky
# to help prevent supply chain attacks. Uses cargo update --dry-run and
# the crates.io API.
#
# Usage:
#   ./scripts/check-updates.sh              # report only (default 7-day quarantine)
#   ./scripts/check-updates.sh --apply      # update safe deps in Cargo.lock
#   QUARANTINE_DAYS=3 ./scripts/check-updates.sh --apply
#
# The --apply flag runs `cargo update -p <crate>` for each dependency that
# passes the quarantine check. This updates Cargo.lock within semver-compatible
# ranges. Major version bumps that require Cargo.toml edits are still manual.
#
set -euo pipefail

QUARANTINE_DAYS="${QUARANTINE_DAYS:-7}"
APPLY=false

for arg in "$@"; do
  case "$arg" in
    --apply) APPLY=true ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

# ── Colors & symbols ──────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'
CHECK='✔'
WARN='⚠'
BLOCK='✖'

# ── Require jq ────────────────────────────────────────────────────────────────
if ! command -v jq &>/dev/null; then
  echo -e "${RED}${BLOCK} jq is required but not found. Install with: brew install jq${RESET}"
  exit 1
fi

# ── Gather outdated deps from cargo ──────────────────────────────────────────
echo -e "${BOLD}${BLUE}Checking for outdated dependencies…${RESET}"
echo ""

raw=$(cargo update --dry-run --verbose 2>&1)
lines=$(echo "$raw" | grep '^\s*Unchanged' || true)

if [ -z "$lines" ]; then
  echo -e "${GREEN}${CHECK} All dependencies are at their latest compatible versions.${RESET}"
  exit 0
fi

# Parse into arrays
declare -a NAMES CURRENT AVAILABLE
while IFS= read -r line; do
  # Format: "   Unchanged crate_name vX.Y.Z (available: vA.B.C)"
  name=$(echo "$line" | sed -E 's/.*Unchanged ([^ ]+).*/\1/')
  cur=$(echo "$line" | sed -E 's/.*Unchanged [^ ]+ v([^ ]+) \(.*/\1/')
  avail=$(echo "$line" | sed -E 's/.*available: v([^)]+).*/\1/')
  NAMES+=("$name")
  CURRENT+=("$cur")
  AVAILABLE+=("$avail")
done <<< "$lines"

count=${#NAMES[@]}

# ── Fetch publish dates from crates.io ───────────────────────────────────────
echo -e "${BOLD}${BLUE}Fetching publish dates for ${count} crate(s) from crates.io…${RESET}"
echo ""

declare -a PUB_DATES AGES STATUSES

now=$(date +%s)

for i in $(seq 0 $((count - 1))); do
  crate="${NAMES[$i]}"
  version="${AVAILABLE[$i]}"

  # crates.io requires a User-Agent header
  response=$(curl -sS -H "User-Agent: readstat-rs-check-updates (https://github.com/curtisalexander/readstat-rs)" \
    "https://crates.io/api/v1/crates/${crate}/${version}" 2>/dev/null || echo '{}')

  created_at=$(echo "$response" | jq -r '.version.created_at // empty' 2>/dev/null || true)

  if [ -n "$created_at" ]; then
    # Parse date — macOS date vs GNU date
    if date -j -f "%Y-%m-%dT%H:%M:%S" "$(echo "$created_at" | cut -c1-19)" +%s &>/dev/null 2>&1; then
      pub_ts=$(date -j -f "%Y-%m-%dT%H:%M:%S" "$(echo "$created_at" | cut -c1-19)" +%s 2>/dev/null)
    else
      pub_ts=$(date -d "$created_at" +%s 2>/dev/null || echo "0")
    fi
    pub_date=$(echo "$created_at" | cut -c1-10)
    age_days=$(( (now - pub_ts) / 86400 ))
  else
    pub_date="unknown"
    age_days=999
  fi

  PUB_DATES+=("$pub_date")
  AGES+=("$age_days")

  if [ "$age_days" -lt "$QUARANTINE_DAYS" ]; then
    STATUSES+=("quarantine")
  else
    STATUSES+=("ok")
  fi

  # Rate-limit: crates.io asks for max 1 req/sec
  sleep 1
done

# ── Print report ─────────────────────────────────────────────────────────────

safe_count=0
quarantine_count=0

# Header
mode_label="report only"
if [ "$APPLY" = true ]; then
  mode_label="apply mode"
fi

echo -e "${BOLD}┌──────────────────────────────────────────────────────────────────────────────────────────────┐${RESET}"
echo -e "${BOLD}│  Outdated Dependencies Report                                                  ${DIM}quarantine: ${QUARANTINE_DAYS}d${RESET}${BOLD}  │${RESET}"
echo -e "${BOLD}├──────────────────────┬───────────────┬───────────────┬──────────────┬─────────┬─────────────┤${RESET}"
printf  "${BOLD}│ %-20s │ %-13s │ %-13s │ %-12s │ %-7s │ %-11s │${RESET}\n" \
        "Crate" "Current" "Available" "Published" "Age" "Status"
echo -e "${BOLD}├──────────────────────┼───────────────┼───────────────┼──────────────┼─────────┼─────────────┤${RESET}"

for i in $(seq 0 $((count - 1))); do
  name="${NAMES[$i]}"
  cur="${CURRENT[$i]}"
  avail="${AVAILABLE[$i]}"
  pub="${PUB_DATES[$i]}"
  age="${AGES[$i]}"
  status="${STATUSES[$i]}"

  age_str="${age}d"

  if [ "$status" = "quarantine" ]; then
    status_str="${RED}${BLOCK} blocked${RESET}"
    age_color="${RED}"
    quarantine_count=$((quarantine_count + 1))
  else
    status_str="${GREEN}${CHECK} safe${RESET}"
    age_color="${GREEN}"
    safe_count=$((safe_count + 1))
  fi

  printf "│ ${CYAN}%-20s${RESET} │ ${DIM}%-13s${RESET} │ ${YELLOW}%-13s${RESET} │ %-12s │ ${age_color}%-7s${RESET} │ %-11b │\n" \
         "$name" "$cur" "$avail" "$pub" "$age_str" "$status_str"
done

echo -e "${BOLD}└──────────────────────┴───────────────┴───────────────┴──────────────┴─────────┴─────────────┘${RESET}"

# Summary
echo ""
echo -e "${BOLD}Summary${RESET}"
echo -e "  ${GREEN}${CHECK}${RESET} ${safe_count} update(s) safe to apply (published ≥ ${QUARANTINE_DAYS} days ago)"
echo -e "  ${RED}${BLOCK}${RESET} ${quarantine_count} update(s) blocked by quarantine (published < ${QUARANTINE_DAYS} days ago)"
echo ""

if [ "$quarantine_count" -gt 0 ]; then
  echo -e "${YELLOW}${WARN} Quarantined updates were published too recently.${RESET}"
  echo -e "  Wait until they are at least ${QUARANTINE_DAYS} days old before upgrading."
  echo -e "  This buffer allows security scanners (cargo-audit, cargo-deny, RustSec)"
  echo -e "  to flag any malicious or compromised releases."
  echo ""
fi

# ── Apply mode ───────────────────────────────────────────────────────────────
if [ "$APPLY" = true ]; then
  if [ "$safe_count" -eq 0 ]; then
    echo -e "${DIM}Nothing to apply — all updates are quarantined.${RESET}"
    exit 0
  fi

  echo -e "${BOLD}${BLUE}Applying ${safe_count} safe update(s) via cargo update…${RESET}"
  echo ""

  applied=0
  skipped=0

  for i in $(seq 0 $((count - 1))); do
    name="${NAMES[$i]}"
    status="${STATUSES[$i]}"
    avail="${AVAILABLE[$i]}"

    if [ "$status" = "quarantine" ]; then
      echo -e "  ${RED}${BLOCK}${RESET} ${DIM}Skipping${RESET} ${CYAN}${name}${RESET} ${DIM}(quarantined)${RESET}"
      skipped=$((skipped + 1))
      continue
    fi

    echo -ne "  ${YELLOW}↻${RESET} Updating ${CYAN}${name}${RESET} → ${YELLOW}${avail}${RESET}…"
    if cargo update -p "${name}" 2>/dev/null; then
      echo -e " ${GREEN}${CHECK}${RESET}"
      applied=$((applied + 1))
    else
      echo -e " ${RED}${BLOCK} failed${RESET}"
    fi
  done

  echo ""
  echo -e "${BOLD}Apply complete${RESET}"
  echo -e "  ${GREEN}${CHECK}${RESET} ${applied} crate(s) updated in Cargo.lock"
  if [ "$skipped" -gt 0 ]; then
    echo -e "  ${RED}${BLOCK}${RESET} ${skipped} crate(s) skipped (quarantined)"
  fi
  echo ""
  echo -e "${DIM}Note: Only Cargo.lock was updated (semver-compatible range).${RESET}"
  echo -e "${DIM}Major version bumps require manual Cargo.toml edits.${RESET}"
else
  echo -e "${DIM}Run with ${BOLD}--apply${RESET}${DIM} to update safe dependencies in Cargo.lock.${RESET}"
fi

# Recommend complementary tools
echo ""
echo -e "${DIM}Tip: Pair this with 'cargo audit' and 'cargo deny check' for full supply chain coverage.${RESET}"
