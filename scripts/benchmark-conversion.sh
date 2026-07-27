#!/usr/bin/env bash
# Benchmark the end-to-end SAS-to-Parquet conversion pipeline.
#
# Usage:
#   ./scripts/benchmark-conversion.sh           # full benchmark suite
#   ./scripts/benchmark-conversion.sh --quick   # shorter smoke benchmark
#
# Results are written under target/benchmark-results/<timestamp>/.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

REPOSITORY="curtisalexander/readstat-rs"
RELEASE_TAG="benchmark-data-v1"
DATA_DIR="$ROOT_DIR/benchmark-data"
DATASET="$DATA_DIR/readstat_benchmark_v1.sas7bdat"
CHECKSUM="$DATA_DIR/readstat_benchmark_v1.sas7bdat.sha256"
MANIFEST="$DATA_DIR/readstat_benchmark_v1_manifest.txt"
EXPECTED_SHA256="64e39c4ac0a2174cb8d37555f5bd47dba837db083873c609e9bed985d30cbf5b"

TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_DIR="$ROOT_DIR/target/benchmark-results/$TIMESTAMP"
OUTPUT="$ROOT_DIR/target/benchmark-output.parquet"
QUICK=false

case "${1:-}" in
    "") ;;
    --quick) QUICK=true ;;
    -h|--help)
        sed -n '2,8p' "$0"
        exit 0
        ;;
    *)
        echo "Usage: $0 [--quick]" >&2
        exit 2
        ;;
esac

for command in cargo gh git hyperfine; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "error: required command not found: $command" >&2
        if [[ "$command" == "hyperfine" && "$(uname -s)" == "Darwin" ]]; then
            echo "Install it with: brew install hyperfine" >&2
        fi
        exit 1
    fi
done

if command -v sha256sum >/dev/null 2>&1; then
    sha256_file() {
        sha256sum "$1" | awk '{print $1}'
    }
elif command -v shasum >/dev/null 2>&1; then
    sha256_file() {
        shasum -a 256 "$1" | awk '{print $1}'
    }
else
    echo "error: sha256sum (Linux) or shasum (macOS) is required" >&2
    exit 1
fi

mkdir -p "$DATA_DIR" "$RESULTS_DIR"

echo "Downloading benchmark corpus (existing assets are retained)..."
gh release download "$RELEASE_TAG" \
    --repo "$REPOSITORY" \
    --pattern 'readstat_benchmark_v1.sas7bdat' \
    --pattern 'readstat_benchmark_v1.sas7bdat.sha256' \
    --pattern 'readstat_benchmark_v1_manifest.txt' \
    --dir "$DATA_DIR" \
    --skip-existing

for path in "$DATASET" "$CHECKSUM" "$MANIFEST"; do
    if [[ ! -s "$path" ]]; then
        echo "error: benchmark asset is missing or empty: $path" >&2
        exit 1
    fi
done

actual_sha256="$(sha256_file "$DATASET" | tr '[:upper:]' '[:lower:]')"
if [[ "$actual_sha256" != "$EXPECTED_SHA256" ]]; then
    echo "error: benchmark dataset checksum mismatch" >&2
    echo "expected: $EXPECTED_SHA256" >&2
    echo "actual:   $actual_sha256" >&2
    exit 1
fi
echo "Checksum verified: $actual_sha256"

physical_cpus=1
logical_cpus=1
memory_bytes="unknown"
case "$(uname -s)" in
    Darwin)
        physical_cpus="$(sysctl -n hw.physicalcpu)"
        logical_cpus="$(sysctl -n hw.logicalcpu)"
        memory_bytes="$(sysctl -n hw.memsize)"
        ;;
    Linux)
        logical_cpus="$(nproc)"
        if command -v lscpu >/dev/null 2>&1; then
            sockets="$(lscpu -p=SOCKET 2>/dev/null | grep -v '^#' | sort -u | wc -l | tr -d ' ')"
            cores_per_socket="$(lscpu -p=CORE,SOCKET 2>/dev/null | grep -v '^#' | sort -u | wc -l | tr -d ' ')"
            if ((sockets > 0 && cores_per_socket > 0)); then
                physical_cpus="$cores_per_socket"
            fi
        fi
        if [[ -r /proc/meminfo ]]; then
            memory_kib="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
            memory_bytes="$((memory_kib * 1024))"
        fi
        ;;
esac

{
    echo "Benchmark timestamp (UTC): $TIMESTAMP"
    echo "Repository: $REPOSITORY"
    echo "Commit: $(git rev-parse HEAD)"
    echo "Working tree:"
    git status --short || true
    echo
    uname -a
    if [[ "$(uname -s)" == "Darwin" ]]; then
        sw_vers
    elif command -v lscpu >/dev/null 2>&1; then
        lscpu
    fi
    echo
    echo "Physical CPUs: $physical_cpus"
    echo "Logical CPUs:  $logical_cpus"
    echo "Memory bytes:  $memory_bytes"
    echo "Dataset bytes: $(wc -c <"$DATASET" | tr -d ' ')"
    echo "Dataset SHA-256: $actual_sha256"
    echo
    rustc -Vv
    cargo -V
    hyperfine --version
    echo
    df -h "$ROOT_DIR"
} | tee "$RESULTS_DIR/machine.txt"

echo
echo "Building optimized CLI..."
cargo build --release -p readstat-cli

if [[ "$QUICK" == true ]]; then
    runs=3
    warmup=0
    stream_rows_values="10000 50000"
else
    runs=7
    warmup=1
    stream_rows_values="5000 10000 25000 50000 100000"
fi

binary="./target/release/readstat"
dataset="benchmark-data/readstat_benchmark_v1.sas7bdat"
benchmark_output="target/benchmark-output.parquet"
prepare="rm -f '$benchmark_output'"
cleanup="$prepare"
serial_command="$binary convert $dataset --output $benchmark_output --overwrite --no-progress --stream-rows 10000"
parallel_command="$serial_command --parallel-write"

cleanup_output() {
    rm -f "$OUTPUT"
}
trap cleanup_output EXIT

echo
echo "Running serial versus parallel Parquet benchmark..."
hyperfine \
    --warmup "$warmup" \
    --runs "$runs" \
    --prepare "$prepare" \
    --cleanup "$cleanup" \
    --export-json "$RESULTS_DIR/parquet-writer.json" \
    --export-markdown "$RESULTS_DIR/parquet-writer.md" \
    --command-name serial-parquet "$serial_command" \
    --command-name parallel-parquet "$parallel_command"

echo
echo "Measuring peak memory..."
cleanup_output
if [[ "$(uname -s)" == "Darwin" ]]; then
    /usr/bin/time -l $serial_command \
        >"$RESULTS_DIR/serial-memory.stdout" \
        2>"$RESULTS_DIR/serial-memory.txt"
    cleanup_output
    /usr/bin/time -l $parallel_command \
        >"$RESULTS_DIR/parallel-memory.stdout" \
        2>"$RESULTS_DIR/parallel-memory.txt"
    memory_pattern='real|user|sys|maximum resident set size|peak memory footprint'
else
    /usr/bin/time -v $serial_command \
        >"$RESULTS_DIR/serial-memory.stdout" \
        2>"$RESULTS_DIR/serial-memory.txt"
    cleanup_output
    /usr/bin/time -v $parallel_command \
        >"$RESULTS_DIR/parallel-memory.stdout" \
        2>"$RESULTS_DIR/parallel-memory.txt"
    memory_pattern='Elapsed|User time|System time|Maximum resident set size'
fi
cleanup_output

{
    echo "Serial:"
    grep -Ei "$memory_pattern" "$RESULTS_DIR/serial-memory.txt" || true
    echo
    echo "Parallel:"
    grep -Ei "$memory_pattern" "$RESULTS_DIR/parallel-memory.txt" || true
} | tee "$RESULTS_DIR/memory-summary.txt"

thread_counts=""
add_thread_count() {
    local value="$1"
    if ((value < 1)); then
        return
    fi
    case " $thread_counts " in
        *" $value "*) ;;
        *) thread_counts="${thread_counts:+$thread_counts }$value" ;;
    esac
}

if [[ -n "${BENCHMARK_THREAD_COUNTS:-}" ]]; then
    thread_counts="${BENCHMARK_THREAD_COUNTS//,/ }"
elif [[ "$QUICK" == true ]]; then
    add_thread_count 1
    add_thread_count "$physical_cpus"
    add_thread_count "$logical_cpus"
else
    power=1
    while ((power <= logical_cpus)); do
        add_thread_count "$power"
        power=$((power * 2))
    done
    add_thread_count "$((physical_cpus - 1))"
    add_thread_count "$physical_cpus"
    add_thread_count "$((logical_cpus - 1))"
    add_thread_count "$logical_cpus"
fi

echo
echo "Sweeping Rayon worker counts: $thread_counts"
for threads in $thread_counts; do
    hyperfine \
        --warmup "$warmup" \
        --runs "$runs" \
        --prepare "$prepare" \
        --cleanup "$cleanup" \
        --export-json "$RESULTS_DIR/parallel-threads-${threads}.json" \
        --export-markdown "$RESULTS_DIR/parallel-threads-${threads}.md" \
        --command-name "parallel-${threads}-workers" \
        "RAYON_NUM_THREADS=$threads $parallel_command"
done

echo
echo "Sweeping input batch sizes: $stream_rows_values"
for stream_rows in $stream_rows_values; do
    serial_rows_command="$binary convert $dataset --output $benchmark_output --overwrite --no-progress --stream-rows $stream_rows"
    parallel_rows_command="$serial_rows_command --parallel-write"
    hyperfine \
        --warmup "$warmup" \
        --runs "$runs" \
        --prepare "$prepare" \
        --cleanup "$cleanup" \
        --export-json "$RESULTS_DIR/stream-rows-${stream_rows}.json" \
        --export-markdown "$RESULTS_DIR/stream-rows-${stream_rows}.md" \
        --command-name "serial-${stream_rows}-rows" "$serial_rows_command" \
        --command-name "parallel-${stream_rows}-rows" "$parallel_rows_command"
done

cleanup_output
trap - EXIT

{
    echo "# Conversion benchmark summary"
    echo
    echo "- Commit: \`$(git rev-parse HEAD)\`"
    echo "- Physical CPUs: $physical_cpus"
    echo "- Logical CPUs: $logical_cpus"
    echo "- Runs per command: $runs"
    echo "- Warmup runs: $warmup"
    echo
    cat "$RESULTS_DIR/parquet-writer.md"
    echo
    echo '```text'
    cat "$RESULTS_DIR/memory-summary.txt"
    echo '```'
    echo
    echo "## Rayon worker sweep"
    for threads in $thread_counts; do
        echo
        echo "### $threads workers"
        echo
        cat "$RESULTS_DIR/parallel-threads-${threads}.md"
    done
    echo
    echo "## Input batch-size sweep"
    for stream_rows in $stream_rows_values; do
        echo
        echo "### $stream_rows rows per input batch"
        echo
        cat "$RESULTS_DIR/stream-rows-${stream_rows}.md"
    done
} >"$RESULTS_DIR/summary.md"

echo
echo "Benchmark complete. Results:"
echo "  $RESULTS_DIR"
echo
echo "Start with:"
echo "  cat '$RESULTS_DIR/summary.md'"
