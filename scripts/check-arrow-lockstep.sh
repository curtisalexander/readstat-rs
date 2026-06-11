#!/usr/bin/env bash
#
# check-arrow-lockstep.sh — Fail if the resolved dependency tree contains more
# than one MAJOR version of `arrow` or `parquet`.
#
# Why this exists:
#   The Arrow ecosystem and DataFusion move in lockstep — each `datafusion`
#   release pins one specific `arrow` major (e.g. datafusion 54 requires
#   arrow ^58). If you bump `arrow`/`parquet` ahead of a `datafusion` release
#   that supports the new major, Cargo does NOT error: it silently resolves
#   BOTH majors into the tree (datafusion's old arrow + your new arrow). It even
#   compiles — until an arrow type crosses the datafusion boundary in the `sql`
#   feature, where it fails with a baffling "expected arrow X, found arrow Y".
#
#   This guard turns that latent footgun into a fast, deterministic failure by
#   reading Cargo.lock directly. It's run in CI (every PR) and at the end of
#   `scripts/check-updates.sh`.
#
# Usage:
#   ./scripts/check-arrow-lockstep.sh
#
# Exit status: 0 if every guarded crate has a single major; 1 on a split.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCK="${1:-$REPO_ROOT/Cargo.lock}"

if [ ! -f "$LOCK" ]; then
  echo "check-arrow-lockstep: Cargo.lock not found at $LOCK" >&2
  echo "  Run 'cargo generate-lockfile' or build first." >&2
  exit 1
fi

# Crates we depend on directly that datafusion also depends on. A major-version
# split in any of these is the symptom of an arrow-ahead-of-datafusion bump.
GUARDED=(arrow parquet)

fail=0
for crate in "${GUARDED[@]}"; do
  # Pull every version of this EXACT crate name from Cargo.lock, reduce to the
  # major component, and dedup. (arrow-array, arrow-schema, etc. are pinned with
  # `arrow` via [workspace.dependencies], so `arrow` itself is a sound canary.)
  majors=$(awk -v c="$crate" '
    /^\[\[package\]\]/      { name=""; ver="" }
    /^name = /             { gsub(/"/, ""); name=$3 }
    /^version = /          { gsub(/"/, ""); if (name==c) print $3 }
  ' "$LOCK" | awk -F. 'NF{print $1}' | sort -u)

  count=$(printf '%s\n' "$majors" | grep -c . || true)
  if [ "$count" -gt 1 ]; then
    echo "✖ Multiple major versions of '$crate' in Cargo.lock: $(echo $majors | tr '\n' ' ')" >&2
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  cat >&2 <<'MSG'

The Arrow ecosystem must stay in lockstep with DataFusion: each datafusion
release pins one arrow major, so bumping arrow/parquet ahead of a datafusion
release that supports the new major splits the tree into two arrow majors and
breaks the `sql` feature (arrow types cross the datafusion boundary).

Fix one of:
  • Hold arrow/parquet at the major datafusion currently requires, or
  • Bump datafusion to a release that supports the new arrow major — together.

Check what datafusion requires:
  curl -s https://crates.io/api/v1/crates/datafusion/<ver>/dependencies \
    | jq -r '.dependencies[] | select(.crate_id=="arrow") | .req'
MSG
  exit 1
fi

echo "✔ Single major version of arrow and parquet in Cargo.lock (lockstep intact)."
