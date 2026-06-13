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
#      editing the version requirement in Cargo.toml by hand. When arrow,
#      parquet, or datafusion is among them, the script queries crates.io to
#      resolve which arrow major the latest datafusion requires and prints a
#      verdict on whether the set can move together yet — no manual curl needed.
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

# Shared crates.io User-Agent (crates.io asks API clients to identify themselves).
CRATES_UA="readstat-rs-check-updates (https://github.com/curtisalexander/readstat-rs)"

# Inner width of the report table (must match the column rule below:
# (20+2)+(13+2)+(13+2)+(12+2)+(7+2)+(11+2) + 5 column joints = 93).
TABLE_INNER=93

# Repeat a (possibly multi-byte) character $1 times.
hbar() { local n=$1 ch=$2 out=''; while ((n-- > 0)); do out+="$ch"; done; printf '%s' "$out"; }

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
# Echoes "date|age_days"; "unknown|-1" on failure. The negative age is a
# fail-closed sentinel: a crate whose publish date we couldn't verify is treated
# as freshly published (quarantined), never as old-and-safe.
fetch_pub_date() {
  local crate="$1" version="$2" now resp created pub_date pub_ts age
  now=$(date +%s)
  resp=$(curl -sS -H "User-Agent: $CRATES_UA" \
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
    echo "unknown|-1"
  fi
}

# Latest stable version of a crate per crates.io (e.g. "54.0.0"). "" on failure.
crates_max_stable() {
  local crate="$1" resp
  resp=$(curl -sS -H "User-Agent: $CRATES_UA" \
    "https://crates.io/api/v1/crates/${crate}" 2>/dev/null || echo '{}')
  echo "$resp" | jq -r '.crate.max_stable_version // .crate.max_version // empty' 2>/dev/null || true
}

# The arrow MAJOR a given (concrete) datafusion version requires, per crates.io.
# This is the lookup the old "run this curl yourself" note asked for, inlined.
# Echoes the bare major (e.g. "58"); "" on failure. The dependencies endpoint
# needs a concrete version (e.g. "54.0.0"), not a bare major.
datafusion_arrow_major() {
  local dfver="$1" resp req
  resp=$(curl -sS -H "User-Agent: $CRATES_UA" \
    "https://crates.io/api/v1/crates/datafusion/${dfver}/dependencies" 2>/dev/null || echo '{}')
  req=$(echo "$resp" | jq -r '.dependencies[]? | select(.crate_id=="arrow") | .req' 2>/dev/null | head -n1)
  # req looks like "^58.3.0", "58", or ">=58.0.0, <59" — take the first integer.
  echo "$req" | grep -oE '[0-9]+' | head -n1
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
  bresp=$(curl -sS -H "User-Agent: $CRATES_UA" \
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
  tleft="  Held-back COMPATIBLE updates"
  tright="quarantine: ${QUARANTINE_DAYS}d  "
  tpad=$(( TABLE_INNER - ${#tleft} - ${#tright} )); (( tpad < 1 )) && tpad=1
  echo -e "${BOLD}┌$(hbar "$TABLE_INNER" '─')┐${RESET}"
  echo -e "${BOLD}│${tleft}$(hbar "$tpad" ' ')${DIM}${tright}${RESET}${BOLD}│${RESET}"
  echo -e "${BOLD}├──────────────────────┬───────────────┬───────────────┬──────────────┬─────────┬─────────────┤${RESET}"
  printf  "${BOLD}│ %-20s │ %-13s │ %-13s │ %-12s │ %-7s │ %-11s │${RESET}\n" \
          "Crate" "Current" "Available" "Published" "Age" "Status"
  echo -e "${BOLD}├──────────────────────┼───────────────┼───────────────┼──────────────┼─────────┼─────────────┤${RESET}"

  for i in $(seq 0 $((compat_count - 1))); do
    # A negative age is the fail-closed sentinel for an unverifiable publish date.
    if [ "${C_AGE[$i]}" -lt 0 ]; then age_str="?"; else age_str="${C_AGE[$i]}d"; fi
    # Plain ASCII status text padded via the format string (color lives OUTSIDE
    # the %-Ns field, so it never affects column width — glyphs are kept to the
    # summary lines below to avoid wide-character padding skew).
    if [ "${C_STATUS[$i]}" = "quarantine" ]; then
      status_text="blocked"; status_color="${RED}"; age_color="${RED}"; quarantine_count=$((quarantine_count + 1))
    else
      status_text="safe"; status_color="${GREEN}"; age_color="${GREEN}"; safe_count=$((safe_count + 1))
    fi
    printf "│ ${CYAN}%-20s${RESET} │ ${DIM}%-13s${RESET} │ ${YELLOW}%-13s${RESET} │ %-12s │ ${age_color}%-7s${RESET} │ ${status_color}%-11s${RESET} │\n" \
           "${C_NAMES[$i]}" "${C_CUR[$i]}" "${C_AVAIL[$i]}" "${C_PUB[$i]}" "$age_str" "$status_text"
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
  echo -e "${DIM}  and run the test suite — APIs may have changed.${RESET}"
  echo ""

  # Arrow/Parquet are pinned to DataFusion: each datafusion release requires one
  # arrow major. Bumping arrow/parquet ahead of a datafusion release that
  # supports the new major silently pulls TWO arrow majors and breaks the `sql`
  # feature. When any of arrow/parquet/datafusion is among the majors above, do
  # the crates.io lookup automatically (the note used to ask you to curl by hand)
  # and print a verdict on whether they can move together yet.
  arrow_target=""        # arrow major we'd be moving to, if pending
  df_pending=false
  for i in $(seq 0 $((major_count - 1))); do
    case "${M_NAMES[$i]}" in
      arrow|parquet) arrow_target="${M_AVAIL[$i]%%.*}" ;;
      datafusion)    df_pending=true ;;
    esac
  done

  if [ -n "$arrow_target" ] || [ "$df_pending" = true ]; then
    echo -e "${YELLOW}  Arrow/Parquet move in lockstep with DataFusion. Resolving compatibility${RESET}"
    echo -e "${YELLOW}  live from crates.io…${RESET}"
    df_latest=$(crates_max_stable datafusion)
    df_latest_arrow=""
    [ -n "$df_latest" ] && df_latest_arrow=$(datafusion_arrow_major "$df_latest")
    # If only datafusion has a pending major, the arrow major it pulls in IS the
    # de-facto target the rest of the set must match.
    [ -z "$arrow_target" ] && arrow_target="$df_latest_arrow"

    if [ -n "$df_latest" ] && [ -n "$df_latest_arrow" ]; then
      echo -e "    latest datafusion ${BOLD}${df_latest}${RESET} requires arrow major ${BOLD}${df_latest_arrow}${RESET}"
      [ -n "$arrow_target" ] && echo -e "    arrow/parquet target major ${BOLD}${arrow_target}${RESET}"
      echo ""
      if [ -n "$arrow_target" ] && [ "$df_latest_arrow" = "$arrow_target" ]; then
        echo -e "  ${GREEN}${CHECK} datafusion ${df_latest} supports arrow ${arrow_target} — bump the whole set together:${RESET}"
        echo -e "${DIM}      arrow/parquet → ${arrow_target} in Cargo.toml [workspace.dependencies]${RESET}"
        echo -e "${DIM}      datafusion    → ${df_latest} in crates/readstat/Cargo.toml${RESET}"
      elif [ -n "$arrow_target" ] && [ "$df_latest_arrow" -lt "$arrow_target" ] 2>/dev/null; then
        echo -e "  ${RED}${BLOCK} No published datafusion release supports arrow ${arrow_target} yet${RESET}"
        echo -e "${YELLOW}      (latest datafusion ${df_latest} still requires arrow ${df_latest_arrow}).${RESET}"
        echo -e "${YELLOW}      Hold arrow/parquet at ${df_latest_arrow} until a datafusion release adds it.${RESET}"
      else
        echo -e "  ${YELLOW}${WARN} latest datafusion ${df_latest} needs arrow ${df_latest_arrow}; review before bumping.${RESET}"
      fi
    else
      echo -e "  ${YELLOW}${WARN} Could not resolve datafusion's arrow requirement from crates.io.${RESET}"
      echo -e "${DIM}      Check manually:${RESET}"
      echo -e "${DIM}      curl -s https://crates.io/api/v1/crates/datafusion/<ver>/dependencies \\\\${RESET}"
      echo -e "${DIM}        | jq -r '.dependencies[] | select(.crate_id==\"arrow\") | .req'${RESET}"
    fi
    echo -e "${DIM}  The 'Arrow/DataFusion lockstep' check below (and CI) enforces this.${RESET}"
    echo ""
  fi
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
  echo -e "  regenerating every target's bindings. In short:"
  echo ""
  echo -e "    ${BOLD}Locally${RESET}  — bump the pin to ${DIM}\"=${BINDGEN_LATEST}\"${RESET} in ${CYAN}Cargo.toml${RESET}, then regenerate"
  echo -e "              your host target and verify it works (needs libclang):"
  echo -e "                ${DIM}cargo build -p readstat-sys --features buildtime_bindgen${RESET}"
  echo -e "                ${DIM}cargo test --workspace${RESET}"
  echo -e "              (Windows also: ${DIM}cargo build -p readstat-iconv-sys --features buildtime_bindgen${RESET})"
  echo -e "    ${BOLD}In CI${RESET}    — push; the ${CYAN}readstat-sys cross-platform CI${RESET} ${CYAN}regen${RESET}/${CYAN}regen-iconv${RESET} jobs"
  echo -e "              regenerate the other targets. Their drift check fails on purpose for"
  echo -e "              each stale file; download the uploaded artifacts, commit them, re-push."
  echo ""
  echo -e "  ${BOLD}Full step-by-step:${RESET} ${CYAN}docs/CI-CD.md${RESET} → \"Updating bindgen … regenerating bindings\""
  echo -e "${DIM}  (--apply will NOT touch bindgen; the exact pin also blocks 'cargo update'.)${RESET}"
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

# ── Arrow/DataFusion lockstep integrity check ─────────────────────────────────
# Catches an arrow/parquet major split in the (possibly just-updated) lockfile.
echo ""
echo -e "${BOLD}${BLUE}Arrow/DataFusion lockstep integrity:${RESET}"
"$SCRIPT_DIR/check-arrow-lockstep.sh" || true

# Recommend complementary tools
echo ""
echo -e "${DIM}Tip: Pair this with 'cargo audit' and 'cargo deny check' for full supply chain coverage.${RESET}"
echo -e "${DIM}Tip: 'cargo update' applies all routine semver-compatible updates at once.${RESET}"
