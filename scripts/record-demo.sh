#!/usr/bin/env bash
# record-demo.sh — Record scripts/demo.sh into an optimized docs/demo.gif.
#
# Drives the whole pipeline reproducibly:
#   1. vhs renders docs/demo.tape into a raw (50fps) GIF.
#   2. ffmpeg downsamples to 20fps with a flat 64-color palette (no dither —
#      terminal output compresses far better without it).
#   3. gifsicle does a final lossy optimization pass.
# A bare `vhs docs/demo.tape` also works but yields the larger raw GIF; this
# script is the canonical, size-optimized path.
#
# Prerequisites (one-time):
#   brew install vhs gifsicle          # vhs pulls in ffmpeg + ttyd
#   cargo build --release -p readstat-cli --features sql
#
# Usage:
#   ./scripts/record-demo.sh           # → docs/demo.gif

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

for tool in vhs ffmpeg gifsicle; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "error: '$tool' not found — install with: brew install vhs gifsicle" >&2
        exit 1
    fi
done

if [ ! -x "target/release/readstat" ]; then
    echo "note: target/release/readstat not found — the demo will fall back to" >&2
    echo "      'cargo run', which is slower and noisier on screen. Build first:" >&2
    echo "      cargo build --release -p readstat-cli --features sql" >&2
fi

RAW="$(mktemp -t readstat-demo-raw.XXXXXX).gif"
PALETTE="$(mktemp -t readstat-demo-pal.XXXXXX).png"
cleanup() { rm -f "$RAW" "$PALETTE"; }
trap cleanup EXIT

echo "==> Recording with vhs (raw)…"
# Override the tape's Output so we optimize into the final docs/demo.gif below.
#
# vhs drives a headless browser against a short-lived ttyd server on an
# ephemeral port. On a cold first launch the browser can occasionally hit
# ttyd before its listener is ready ("could not open ttyd: ...
# ERR_CONNECTION_REFUSED"); the run is fully idempotent, so just retry. Each
# attempt gets a fresh ttyd on a new port, so this is safe to repeat.
attempts=3
for attempt in $(seq 1 "$attempts"); do
    if vhs --output "$RAW" docs/demo.tape; then
        break
    fi
    if [ "$attempt" -eq "$attempts" ]; then
        echo "error: vhs failed after $attempts attempts" >&2
        exit 1
    fi
    echo "   vhs attempt $attempt failed (likely a ttyd startup race) — retrying…" >&2
    pkill -x ttyd 2>/dev/null || true   # clear any stray server from the failed try
    sleep 1
done

echo "==> Downsampling to 20fps (flat 64-color palette, no dither)…"
ffmpeg -y -i "$RAW" -vf "fps=20,palettegen=max_colors=64:stats_mode=diff" "$PALETTE" 2>/dev/null
ffmpeg -y -i "$RAW" -i "$PALETTE" \
    -lavfi "fps=20[x];[x][1:v]paletteuse=dither=none" /tmp/readstat-demo-20.gif 2>/dev/null

echo "==> Final gifsicle optimization pass…"
gifsicle -O3 --lossy=40 /tmp/readstat-demo-20.gif -o docs/demo.gif
rm -f /tmp/readstat-demo-20.gif

echo "==> Done: docs/demo.gif ($(du -h docs/demo.gif | cut -f1))"
