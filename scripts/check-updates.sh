#!/usr/bin/env bash
#
# check-updates.sh — Report outdated workspace dependencies with publish dates.
#
# Surfaces, from `cargo update --dry-run` + the crates.io API:
#   1. Held-back COMPATIBLE updates — a newer semver-compatible version exists
#      but the lock is pinned (often by a transitive constraint). Subject to the
#      quarantine check and applied by --apply.
#   2. MAJOR updates — a newer semver-INCOMPATIBLE version is available. These
#      are reported but NEVER applied automatically: bumping them requires
#      editing the version requirement in Cargo.toml by hand.
#   3. A dedicated `bindgen` advisory. bindgen is exact-pinned ("=x.y.z") because
#      its output drives the checked-in per-target FFI bindings; this script
#      reports a newer bindgen if one exists and prints exactly how to take it.
#
# The quarantine flags versions published < QUARANTINE_DAYS ago as risky, giving
# scanners (cargo-audit, cargo-deny, RustSec) time to flag compromised releases.
#
# Usage:
#   ./scripts/check-updates.sh              # report only (default 7-day quarantine)
#   ./scripts/check-updates.sh --apply      # pull safe COMPATIBLE updates into Cargo.lock
#   QUARANTINE_DAYS=3 ./scripts/check-updates.sh --apply
#
# --apply runs `cargo update -p <crate>` for each COMPATIBLE update that clears
# quarantine — Cargo.lock only, within existing semver ranges. MAJOR bumps and
# bindgen are never applied automatically (see their advisories).
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

# Resolve repo root so the script works from any directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

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
  echo -e "${RED}${BLOCK} jq is required but not found. Install it with one of:${RESET}"
  echo -e "    macOS:          brew install jq"
  echo -e "    Debian/Ubuntu:  sudo apt install jq"
  echo -e "    Fedora:         sudo dnf install jq"
  echo -e "    Windows:        winget install jqlang.jq   (or use the PowerShell script, which needs no jq)"
  echo -e "    other:          https://jqlang.github.io/jq/download/"
  exit 1
fi

# ── Version helpers ───────────────────────────────────────────────────────────

# True if $2 (available) is semver-caret-compatible with $1 (current).
# Pre-release suffixes are ignored. Mirrors Cargo's default `^` semantics:
#   x>=1  → same major;  0.y (y>=1) → same minor;  0.0.z → same patch.
semver_caret_compat() {
  local a="${1%%-*}" b="${2%%-*}"
  local amaj amin apat bmaj bmin bpat
  IFS=. read -r amaj amin apat <<< "$a"
  IFS=. read -r bmaj bmin bpat <<< "$b"
  amin=${amin:-0}; apat=${apat:-0}; bmin=${bmin:-0}; bpat=${bpat:-0}
  if [ "${amaj:-0}" != "0" ]; then
    [ "$amaj" = "$bmaj" ]
  elif [ "${amin:-0}" != "0" ]; then
    [ "$bmaj" = "0" ] && [ "$amin" = "$bmin" ]
  else
    [ "$bmaj" = "0" ] && [ "$bmin" = "0" ] && [ "$apat" = "$bpat" ]
  fi
}

# True if $1 is strictly newer than $2 (version sort, pre-release ignored).
ver_gt() {
  [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "${1%%-*}" "${2%%-*}" | sort -V | tail -n1)" = "${1%%-*}" ]
}

# Fetch the most recent stable publish date (YYYY-MM-DD) for a crate@version.
# Echoes "date|age_days"; "unknown|999" on failure.
fetch_pub_date() {
  local crate="$1" version="$2" now resp created pub_date pub_ts age
  now=$(date +%s)
  resp=$(curl -sS -H "User-Agent: readstat-rs-check-updates (https://github.com/curtisalexander/readstat-rs)" \
    "https://crates.io/api/v1/crates/${crate}/${version}" 2>/dev/null || echo '{}')
  created=$(echo "$resp" | jq -r '.version.created_at // empty' 2>/dev/null || true)
  if [ -n "$created" ]; then
    if date -j -f "%Y-%m-%dT%H:%M:%S" "$(echo "$created" | cut -c1-19)" +%s &>/dev/null 2>&1; then
      pub_ts=$(date -j -f "%Y-%m-%dT%H:%M:%S" "$(echo "$created" | cut -c1-19)" +%s 2>/dev/null)
    else
      pub_ts=$(date -d "$created" +%s 2>/dev/null || echo "0")
    fi
    pub_date=$(echo "$created" | cut -c1-10)
    age=$(( (now - pub_ts) / 86400 ))
    echo "${pub_date}|${age}"
  else
    echo "unknown|999"
  fi
}

# ── Gather candidates from cargo ──────────────────────────────────────────────
echo -e "${BOLD}${BLUE}Checking for outdated dependencies…${RESET}"
echo ""

raw=$(cargo update --dry-run --verbose 2>&1)

# Compatible held-back and major candidates come from every "(available: vX)"
# annotation cargo prints — on both `Updating` and `Unchanged` lines, so a major
# annotated on an `Updating … -> …` line is no longer missed.
declare -a C_NAMES C_CUR C_AVAIL          # compatible, held back
declare -a M_NAMES M_CUR M_AVAIL          # major / incompatible
BINDGEN_CARGO_AVAIL=""                     # bindgen version cargo saw available

while IFS= read -r line; do
  [[ "$line" == *"(available:"* ]] || continue
  # Strip everything up to the verb, then read: <name> v<cur> ... (available: v<avail>)
  if [[ "$line" =~ (Updating|Unchanged)[[:space:]]+([^[:space:]]+)[[:space:]]+v([^[:space:]]+) ]]; then
    name="${BASH_REMATCH[2]}"
    cur="${BASH_REMATCH[3]}"
  else
    continue
  fi
  if [[ "$line" =~ \(available:[[:space:]]*v([^\)]+)\) ]]; then
    avail="${BASH_REMATCH[1]}"
  else
    continue
  fi

  if [ "$name" = "bindgen" ]; then
    BINDGEN_CARGO_AVAIL="$avail"
    continue
  fi

  if semver_caret_compat "$cur" "$avail"; then
    C_NAMES+=("$name"); C_CUR+=("$cur"); C_AVAIL+=("$avail")
  else
    M_NAMES+=("$name"); M_CUR+=("$cur"); M_AVAIL+=("$avail")
  fi
done <<< "$raw"

compat_count=${#C_NAMES[@]}
major_count=${#M_NAMES[@]}

# ── bindgen advisory check (independent of cargo, since it is exact-pinned) ────
BINDGEN_PIN=$(grep -E '^[[:space:]]*bindgen[[:space:]]*=[[:space:]]*"=' Cargo.toml 2>/dev/null \
  | sed -E 's/.*"=([0-9][0-9A-Za-z.+-]*)".*/\1/' | head -n1)
BINDGEN_LATEST=""
if [ -n "$BINDGEN_PIN" ]; then
  bresp=$(curl -sS -H "User-Agent: readstat-rs-check-updates (https://github.com/curtisalexander/readstat-rs)" \
    "https://crates.io/api/v1/crates/bindgen" 2>/dev/null || echo '{}')
  BINDGEN_LATEST=$(echo "$bresp" | jq -r '.crate.max_stable_version // .crate.max_version // empty' 2>/dev/null || true)
  # Prefer whichever is newer: what cargo saw vs crates.io max-stable.
  if [ -n "$BINDGEN_CARGO_AVAIL" ] && ver_gt "$BINDGEN_CARGO_AVAIL" "${BINDGEN_LATEST:-0}"; then
    BINDGEN_LATEST="$BINDGEN_CARGO_AVAIL"
  fi
fi

if [ "$compat_count" -eq 0 ] && [ "$major_count" -eq 0 ] \
   && { [ -z "$BINDGEN_LATEST" ] || ! ver_gt "$BINDGEN_LATEST" "${BINDGEN_PIN:-0}"; }; then
  echo -e "${GREEN}${CHECK} No held-back, major, or bindgen updates available — everything is current.${RESET}"
  echo ""
  echo -e "${DIM}(Routine semver-compatible updates are applied directly with 'cargo update'.)${RESET}"
  exit 0
fi

# ── Quarantine + publish dates for COMPATIBLE held-back updates ───────────────
declare -a C_PUB C_AGE C_STATUS
if [ "$compat_count" -gt 0 ]; then
  echo -e "${BOLD}${BLUE}Fetching publish dates for ${compat_count} compatible update(s)…${RESET}"
  echo ""
  for i in $(seq 0 $((compat_count - 1))); do
    IFS='|' read -r pd ag < <(fetch_pub_date "${C_NAMES[$i]}" "${C_AVAIL[$i]}")
    C_PUB+=("$pd"); C_AGE+=("$ag")
    if [ "$ag" -lt "$QUARANTINE_DAYS" ]; then C_STATUS+=("quarantine"); else C_STATUS+=("ok"); fi
    sleep 1  # crates.io: max ~1 req/sec
  done
fi

# ── Report: compatible held-back updates ──────────────────────────────────────
safe_count=0
quarantine_count=0

if [ "$compat_count" -gt 0 ]; then
  echo -e "${BOLD}┌────────────────────────────────────────────────────────────────────────────────────────────────┐${RESET}"
  echo -e "${BOLD}│  Held-back COMPATIBLE updates                                                  ${DIM}quarantine: ${QUARANTINE_DAYS}d${RESET}${BOLD}    │${RESET}"
  echo -e "${BOLD}├──────────────────────┬───────────────┬───────────────┬──────────────┬─────────┬─────────────┤${RESET}"
  printf  "${BOLD}│ %-20s │ %-13s │ %-13s │ %-12s │ %-7s │ %-11s │${RESET}\n" \
          "Crate" "Current" "Available" "Published" "Age" "Status"
  echo -e "${BOLD}├──────────────────────┼───────────────┼───────────────┼──────────────┼─────────┼─────────────┤${RESET}"

  for i in $(seq 0 $((compat_count - 1))); do
    age_str="${C_AGE[$i]}d"
    if [ "${C_STATUS[$i]}" = "quarantine" ]; then
      status_str="${RED}${BLOCK} blocked${RESET}"; age_color="${RED}"; quarantine_count=$((quarantine_count + 1))
    else
      status_str="${GREEN}${CHECK} safe${RESET}"; age_color="${GREEN}"; safe_count=$((safe_count + 1))
    fi
    printf "│ ${CYAN}%-20s${RESET} │ ${DIM}%-13s${RESET} │ ${YELLOW}%-13s${RESET} │ %-12s │ ${age_color}%-7s${RESET} │ %-11b │\n" \
           "${C_NAMES[$i]}" "${C_CUR[$i]}" "${C_AVAIL[$i]}" "${C_PUB[$i]}" "$age_str" "$status_str"
  done
  echo -e "${BOLD}└──────────────────────┴───────────────┴───────────────┴──────────────┴─────────┴─────────────┘${RESET}"
  echo ""
  echo -e "  ${GREEN}${CHECK}${RESET} ${safe_count} compatible update(s) safe to apply (≥ ${QUARANTINE_DAYS} days old)"
  echo -e "  ${RED}${BLOCK}${RESET} ${quarantine_count} compatible update(s) blocked by quarantine (< ${QUARANTINE_DAYS} days old)"
  echo ""
fi

# ── Report: MAJOR (incompatible) updates — manual only ────────────────────────
if [ "$major_count" -gt 0 ]; then
  echo -e "${BOLD}${YELLOW}${WARN} MAJOR updates available (${major_count}) — not applied automatically${RESET}"
  for i in $(seq 0 $((major_count - 1))); do
    echo -e "  ${CYAN}${M_NAMES[$i]}${RESET}  ${DIM}${M_CUR[$i]}${RESET} ${BOLD}→${RESET} ${YELLOW}${M_AVAIL[$i]}${RESET}"
  done
  echo ""
  echo -e "${DIM}  These cross a semver-incompatible boundary. To take one, bump its version${RESET}"
  echo -e "${DIM}  requirement in the relevant Cargo.toml (e.g. \`foo = \"54\"\`), then \`cargo build\`${RESET}"
  echo -e "${DIM}  and run the test suite — APIs may have changed. For the Arrow/Parquet and${RESET}"
  echo -e "${DIM}  DataFusion crates, bump the whole set together (see CLAUDE.md).${RESET}"
  echo ""
fi

# ── Report: bindgen advisory ──────────────────────────────────────────────────
if [ -n "$BINDGEN_PIN" ] && [ -n "$BINDGEN_LATEST" ] && ver_gt "$BINDGEN_LATEST" "$BINDGEN_PIN"; then
  echo -e "${BOLD}${YELLOW}${WARN} bindgen update available: ${DIM}${BINDGEN_PIN}${RESET} ${BOLD}→${RESET} ${YELLOW}${BINDGEN_LATEST}${RESET}"
  echo -e "${RED}${BOLD}  Do NOT bump bindgen casually.${RESET} It is exact-pinned (\`bindgen = \"=${BINDGEN_PIN}\"\`)"
  echo -e "  in the workspace \`Cargo.toml\` because its generated output drives the"
  echo -e "  checked-in per-target FFI bindings in:"
  echo -e "    ${CYAN}crates/readstat-sys/src/bindings/${RESET}"
  echo -e "    ${CYAN}crates/readstat-iconv-sys/src/bindings/${RESET}"
  echo -e "  A bindgen bump can silently change that output, so it must be paired with"
  echo -e "  regenerating and committing the bindings. To update it:"
  echo ""
  echo -e "    ${BOLD}1.${RESET} Edit the pin in ${CYAN}Cargo.toml${RESET}: ${DIM}bindgen = \"=${BINDGEN_LATEST}\"${RESET}"
  echo -e "    ${BOLD}2.${RESET} Regenerate bindings on ${BOLD}each${RESET} supported target — easiest via CI:"
  echo -e "       push the change and let the ${CYAN}readstat-sys cross-platform CI${RESET} workflow's"
  echo -e "       ${CYAN}regen${RESET} / ${CYAN}regen-iconv${RESET} jobs run, then download each ${DIM}bindings-*${RESET} artifact."
  echo -e "       Locally (current target only): ${DIM}cargo build -p readstat-sys --features buildtime_bindgen${RESET}"
  echo -e "       and ${DIM}cargo build -p readstat-iconv-sys --features buildtime_bindgen${RESET} (Windows, needs libclang)."
  echo -e "    ${BOLD}3.${RESET} Copy the regenerated files into the bindings dirs above and commit them"
  echo -e "       alongside the Cargo.toml change."
  echo -e "    ${BOLD}4.${RESET} CI's drift check (${CYAN}readstat-sys-ci.yml${RESET}) must pass — it fails if the"
  echo -e "       committed bindings differ from a fresh regeneration."
  echo ""
  echo -e "${DIM}  --apply will NOT touch bindgen; the exact pin also prevents 'cargo update'${RESET}"
  echo -e "${DIM}  from moving it. This is intentional.${RESET}"
  echo ""
fi

if [ "$quarantine_count" -gt 0 ]; then
  echo -e "${YELLOW}${WARN} Quarantined updates were published too recently.${RESET}"
  echo -e "  Wait until they are at least ${QUARANTINE_DAYS} days old before upgrading."
  echo -e "  This buffer allows security scanners (cargo-audit, cargo-deny, RustSec)"
  echo -e "  to flag any malicious or compromised releases."
  echo ""
fi

# ── Apply mode (compatible + safe only) ───────────────────────────────────────
if [ "$APPLY" = true ]; then
  if [ "$compat_count" -eq 0 ] || [ "$safe_count" -eq 0 ]; then
    echo -e "${DIM}Nothing to apply — no compatible updates cleared quarantine.${RESET}"
  else
    echo -e "${BOLD}${BLUE}Applying ${safe_count} safe compatible update(s) via cargo update…${RESET}"
    echo ""
    applied=0; skipped=0
    for i in $(seq 0 $((compat_count - 1))); do
      if [ "${C_STATUS[$i]}" = "quarantine" ]; then
        echo -e "  ${RED}${BLOCK}${RESET} ${DIM}Skipping${RESET} ${CYAN}${C_NAMES[$i]}${RESET} ${DIM}(quarantined)${RESET}"
        skipped=$((skipped + 1)); continue
      fi
      echo -ne "  ${YELLOW}↻${RESET} Updating ${CYAN}${C_NAMES[$i]}${RESET} → ${YELLOW}${C_AVAIL[$i]}${RESET}…"
      if cargo update -p "${C_NAMES[$i]}" --precise "${C_AVAIL[$i]}" 2>/dev/null \
         || cargo update -p "${C_NAMES[$i]}" 2>/dev/null; then
        echo -e " ${GREEN}${CHECK}${RESET}"; applied=$((applied + 1))
      else
        echo -e " ${RED}${BLOCK} held back (likely a transitive constraint)${RESET}"
      fi
    done
    echo ""
    echo -e "${BOLD}Apply complete${RESET}"
    echo -e "  ${GREEN}${CHECK}${RESET} ${applied} crate(s) updated in Cargo.lock"
    [ "$skipped" -gt 0 ] && echo -e "  ${RED}${BLOCK}${RESET} ${skipped} crate(s) skipped (quarantined)"
    echo ""
    echo -e "${DIM}Note: Only Cargo.lock was updated (semver-compatible range).${RESET}"
    echo -e "${DIM}MAJOR bumps and bindgen require the manual steps described above.${RESET}"
  fi
else
  echo -e "${DIM}Run with ${BOLD}--apply${RESET}${DIM} to pull safe compatible updates into Cargo.lock.${RESET}"
fi

# Recommend complementary tools
echo ""
echo -e "${DIM}Tip: Pair this with 'cargo audit' and 'cargo deny check' for full supply chain coverage.${RESET}"
echo -e "${DIM}Tip: 'cargo update' applies all routine semver-compatible updates at once.${RESET}"
