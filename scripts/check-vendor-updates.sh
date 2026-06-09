#!/usr/bin/env bash
#
# check-vendor-updates.sh — Report upstream updates to the vendored git submodules
# WITHOUT altering them.
#
# Read-only: uses `git ls-remote` to query each submodule's upstream, which
# contacts the remote and prints refs but writes NOTHING locally — no fetch, no
# pull, no checkout. The pinned commit recorded by the superproject is never
# touched, so `git status` stays clean.
#
# For each submodule (from .gitmodules) it reports:
#   • the currently pinned commit + nearest tag,
#   • whether the upstream default branch has moved past the pin, and
#   • whether a newer release tag exists upstream.
#
# Note: an exact "commits behind" count would require fetching objects (which
# this script deliberately does not do), so the default-branch comparison is
# reported as same / moved rather than a number.
#
# Usage:
#   ./scripts/check-vendor-updates.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'; BLUE='\033[0;34m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; RESET='\033[0m'
CHECK='✔'; WARN='⚠'; BLOCK='✖'

# True if $1 is strictly newer than $2 (leading 'v' and pre-release/build ignored).
ver_gt() {
  local a="${1#v}" b="${2#v}"
  a="${a%%+*}"; b="${b%%+*}"
  [ "$a" != "$b" ] && [ "$(printf '%s\n%s\n' "$a" "$b" | sort -V | tail -n1)" = "$a" ]
}

if [ ! -f .gitmodules ]; then
  echo -e "${RED}${BLOCK} No .gitmodules found at repo root.${RESET}"
  exit 1
fi

echo -e "${BOLD}${BLUE}Checking vendored submodules for upstream updates (read-only)…${RESET}"
echo ""

# Submodule config keys look like: submodule.<name>.path / submodule.<name>.url
names=$(git config -f .gitmodules --name-only --get-regexp '\.path$' | sed -E 's/\.path$//')

any_update=false

while IFS= read -r key; do
  [ -n "$key" ] || continue
  path=$(git config -f .gitmodules --get "${key}.path")
  url=$(git config -f .gitmodules --get "${key}.url")
  short_name="${path##*/}"

  echo -e "${BOLD}${CYAN}${short_name}${RESET} ${DIM}(${path})${RESET}"
  echo -e "  ${DIM}url:${RESET}            ${url}"

  if [ ! -d "$path/.git" ] && [ ! -f "$path/.git" ]; then
    echo -e "  ${YELLOW}${WARN} submodule not initialized — run: git submodule update --init '${path}'${RESET}"
    echo ""
    continue
  fi

  cur_sha=$(git -C "$path" rev-parse HEAD 2>/dev/null || echo "")
  cur_describe=$(git -C "$path" describe --tags --always 2>/dev/null || echo "?")
  cur_tag=$(git -C "$path" describe --tags --abbrev=0 2>/dev/null || echo "")
  echo -e "  ${DIM}pinned commit:${RESET}  ${cur_sha:0:9}  ${DIM}(${cur_describe})${RESET}"

  # --- upstream default branch tip (read-only) ---
  symref=$(git ls-remote --symref "$url" HEAD 2>/dev/null || echo "")
  remote_head=$(echo "$symref" | awk '$2=="HEAD"{print $1; exit}')
  def_branch=$(echo "$symref" | sed -nE 's#^ref: refs/heads/([^[:space:]]+)[[:space:]]+HEAD$#\1#p')
  : "${def_branch:=default}"

  if [ -z "$remote_head" ]; then
    echo -e "  ${RED}${BLOCK} could not reach upstream (network/remote error)${RESET}"
    echo ""
    continue
  fi

  if [ "$remote_head" = "$cur_sha" ]; then
    echo -e "  ${DIM}upstream ${def_branch}:${RESET}  ${remote_head:0:9}  ${GREEN}${CHECK} pin is at the branch tip${RESET}"
  else
    echo -e "  ${DIM}upstream ${def_branch}:${RESET}  ${remote_head:0:9}  ${YELLOW}${WARN} branch has moved (newer commits upstream; fetch to see them)${RESET}"
    any_update=true
  fi

  # --- latest upstream release tag (read-only) ---
  latest_tag=$(git ls-remote --tags --refs "$url" 2>/dev/null \
    | awk '{print $2}' | sed 's#refs/tags/##' \
    | grep -E '^v?[0-9]+' | sort -V | tail -n1 || echo "")

  if [ -z "$latest_tag" ]; then
    echo -e "  ${DIM}latest tag:${RESET}     ${DIM}(no version tags upstream)${RESET}"
  elif [ -z "$cur_tag" ]; then
    echo -e "  ${DIM}latest tag:${RESET}     ${YELLOW}${latest_tag}${RESET}  ${DIM}(pin has no nearest tag)${RESET}"
  elif ver_gt "$latest_tag" "$cur_tag"; then
    echo -e "  ${DIM}latest tag:${RESET}     ${YELLOW}${latest_tag}${RESET}  ${YELLOW}${WARN} newer release than pinned ${cur_tag}${RESET}"
    any_update=true
  else
    echo -e "  ${DIM}latest tag:${RESET}     ${latest_tag}  ${GREEN}${CHECK} pin is at or past the latest tag${RESET}"
  fi
  echo ""
done <<< "$names"

if [ "$any_update" = true ]; then
  echo -e "${YELLOW}${WARN} Upstream changes are available for one or more submodules.${RESET}"
  echo -e "${DIM}  To adopt one (this DOES alter the vendored checkout):${RESET}"
  echo -e "${DIM}    git -C <path> fetch origin${RESET}"
  echo -e "${DIM}    git -C <path> checkout <commit-or-tag>   # then commit the submodule bump${RESET}"
  echo -e "${DIM}  Bumping the vendored C also requires regenerating + committing the per-target${RESET}"
  echo -e "${DIM}  bindings (cargo build -p readstat-sys --features buildtime_bindgen, etc.) —${RESET}"
  echo -e "${DIM}  CI's drift check enforces this. See docs/RELEASING.md / CHANGELOG.md.${RESET}"
else
  echo -e "${GREEN}${CHECK} All vendored submodules are at or ahead of upstream's latest tag and branch tip.${RESET}"
fi
