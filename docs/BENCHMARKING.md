[< Back to README](../README.md)

# Performance Benchmarking with Criterion

## Overview

This document provides a comprehensive guide to performance benchmarking in readstat-rs using [Criterion.rs](https://github.com/bheisler/criterion.rs).

## Quick Start

```bash
# Run all benchmarks (from the repository root)
cargo bench -p readstat

# View HTML reports (Criterion writes to the workspace-root target/)
open target/criterion/report/index.html
```

## What Gets Benchmarked

### 1. Reading Performance
- **Metadata Reading** (`~300-950 µs`) - File header parsing
- **Single Chunk Reading** - Full dataset read performance
- **Chunked Reading** - Streaming with different chunk sizes (1K, 5K, 10K rows)

### 2. Data Conversion
- **Arrow Conversion** - SAS types → Arrow RecordBatch overhead

### 3. Writing Performance
- **CSV Writing** - Text format output
- **Parquet Compression** - Uncompressed, Snappy, Zstd comparison
- **Format Comparison** - CSV vs Parquet vs Feather vs NDJSON

### 4. Parallel Write Optimization
- **Buffer Sizes** - SpooledTempFile memory thresholds (1MB, 10MB, 100MB, 500MB)

### 5. End-to-End Pipeline
- **Complete Conversion** - Read + Write combined (most important)

## Sample Results

From initial benchmark run (example output):

```
metadata_reading/all_types.sas7bdat
                        time:   [299.41 µs 301.84 µs 304.29 µs]

metadata_reading/cars.sas7bdat
                        time:   [935.21 µs 943.52 µs 952.41 µs]

read_single_chunk/cars.sas7bdat
                        time:   [~2-3 ms]
                        thrpt:  [~150-200K rows/sec]

write_parquet_compression/snappy
                        time:   [~4-6 ms]
                        thrpt:  [~70-100K rows/sec]

end_to_end_conversion/parquet
                        time:   [~6-9 ms]
                        thrpt:  [~50-70K rows/sec]
```

## Interpreting Results

### Understanding the Output

**Time Measurement:**
```
time: [299.41 µs 301.84 µs 304.29 µs]
       ^         ^         ^
       |         |         +-- Upper bound (95% confidence)
       |         +------------ Median
       +---------------------- Lower bound (95% confidence)
```

**Throughput:**
```
thrpt: [150K elem/s 175K elem/s 200K elem/s]
        ^           ^           ^
        |           |           +-- Upper bound
        |           +-------------- Median
        +-------------------------- Lower bound
```

**Change Detection:**
```
change: [-2.3456% -1.2345% +0.1234%] (p = 0.12 > 0.05)
         ^         ^         ^        ^
         |         |         |        +-- Statistical significance
         |         |         +----------- Upper bound of change
         |         +--------------------- Median change
         +------------------------------- Lower bound of change
```

### What to Look For

#### 🔴 Red Flags (Investigate)
- **High variance** (>10%) - Results unreliable
- **Significant regression** (>5% slower, p < 0.05)
- **Outliers** (>5% of samples)

#### 🟡 Opportunities
- **Chunked reading** - Test if different chunk size improves throughput
- **Buffer sizes** - If small buffer performs as well as large, save memory
- **Compression** - If uncompressed only slightly faster, use compression

#### 🟢 Validation
- **Low variance** (<5%) - Reliable results
- **Improvements** (>10% faster, p < 0.05)
- **Expected patterns** (e.g., compression should be slower but smaller)

## Performance Optimization Workflow

### Step 1: Establish Baseline
```bash
# Save current performance as baseline
cargo bench --save-baseline main

# Results saved to target/criterion/{benchmark}/main/
```

### Step 2: Make Changes
Edit code with optimization hypothesis:
- Increase buffer size
- Change algorithm
- Add caching
- Parallel processing

### Step 3: Measure Impact
```bash
# Compare against baseline
cargo bench --baseline main

# Look for "change: [X% Y% Z%]" in output
```

### Step 4: Analyze & Iterate

**If improved (>10%, p < 0.05):**
✅ Keep the change
✅ Update baseline: `cargo bench --save-baseline main`

**If no change (<5%):**
⚠️ Optimization didn't help - profile to find real bottleneck

**If regressed (slower):**
❌ Revert change
❌ Investigate why performance decreased

## Common Optimization Scenarios

### Scenario 1: Slow Reading
**Symptoms:** `read_single_chunk` time is high

**Investigate:**
1. ReadStat C library overhead (FFI calls)
2. Memory allocation patterns
3. Callback overhead

**Try:**
- Larger buffers in C library
- Memory-mapped files (see evaluation doc)
- Pre-allocate column vectors

### Scenario 2: Slow Writing
**Symptoms:** `write_formats` time is high

**Investigate:**
1. BufWriter buffer size
2. Format-specific overhead
3. Compression CPU usage

**Try:**
- Increase BufWriter capacity (currently 8KB)
- Use faster compression (Snappy vs Zstd)
- Parallel writing (already implemented)

### Scenario 3: Memory Issues
**Symptoms:** System swapping, OOM errors

**Investigate:**
1. Chunk size too large
2. Too many parallel streams
3. Memory leaks

**Try:**
- Reduce `stream_rows` (default 10,000)
- Reduce parallel write buffer (default 100MB)
- Use bounded channels (already implemented)

### Scenario 4: High Variance
**Symptoms:** Large confidence intervals, many outliers

**Investigate:**
1. System background activity
2. CPU frequency scaling
3. Thermal throttling

**Try:**
- Close background apps
- Disable frequency scaling
- Run on consistent power mode

## Advanced Profiling

### CPU Profiling with Flamegraphs
```bash
# Install flamegraph
cargo install flamegraph

# Profile a specific benchmark
cargo flamegraph --bench readstat_benchmarks -- --bench read_single_chunk

# Open flamegraph.svg to see hotspots
```

**What to look for:**
- Wide bars = lots of time spent
- Deep stacks = call overhead
- Unexpected functions = bugs/inefficiency

### Memory Profiling
```bash
# Using valgrind (Linux)
valgrind --tool=massif \
  cargo bench read_single_chunk --no-run
ms_print massif.out.* > memory_profile.txt

# Using heaptrack (Linux)
heaptrack cargo bench read_single_chunk
heaptrack_gui heaptrack.*.gz
```

### System Call Tracing
```bash
# Linux: strace
strace -c cargo bench read_single_chunk 2>&1 | tail -20

# macOS: dtruss
sudo dtruss -c cargo bench read_single_chunk
```

## Comparing Implementations

### Before/After Memory-Mapped Files
```bash
# Baseline without mmap
git checkout main
cargo bench --save-baseline without-mmap

# With mmap implementation
git checkout feature/mmap
cargo bench --baseline without-mmap

# Look for improvements in read_single_chunk
```

### Parallel vs Sequential
```bash
# Test with different parallelism settings
cargo bench end_to_end -- --parallel
cargo bench end_to_end -- --sequential
```

## CI/CD Integration

### Performance Regression Detection

Add to `.github/workflows/benchmarks.yml`:

```yaml
name: Performance Benchmarks

on:
  pull_request:
    branches: [main]

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Rust
        uses: dtolnay/rust-toolchain@stable

      - name: Run benchmarks
        run: |
          cd crates/readstat
          cargo bench --no-run  # Just compile for CI

      - name: Compare with baseline (on main branch)
        if: github.event_name == 'pull_request'
        run: |
          git fetch origin main:main
          git checkout main
          cargo bench --save-baseline main
          git checkout -
          cargo bench --baseline main
```

## Best Practices

### Do's ✅
- Run benchmarks on consistent hardware
- Close background applications
- Use `--save-baseline` for comparisons
- Profile after benchmarking to find bottlenecks
- Document performance changes in PRs
- Test on representative data sizes

### Don'ts ❌
- Don't benchmark on laptop (throttling)
- Don't optimize without profiling first
- Don't trust results with high variance
- Don't compare across different systems
- Don't commit benchmark artifacts
- Don't skip statistical significance checks

## Performance Goals

### Current Performance (Baseline)
- **Metadata reading**: ~300-950 µs
- **Read throughput**: ~150-200K rows/sec
- **Write throughput**: ~70-100K rows/sec
- **End-to-end**: ~50-70K rows/sec

### Target Performance (Goals)
- **Metadata reading**: <500 µs (↓30%)
- **Read throughput**: >250K rows/sec (↑25%)
- **Write throughput**: >100K rows/sec (↑30%)
- **End-to-end**: >100K rows/sec (↑40%)

### Stretch Goals
- **Memory-mapped reads**: 2x faster for large files
- **Parallel writes**: 3-4x speedup with 4+ cores
- **Compression**: <10% overhead for Snappy

## Data Files for Benchmarking

### Current Test Data
- **all_types.sas7bdat** - 3 rows, 10 vars (tiny)
- **cars.sas7bdat** - 1081 rows, 13 vars (small)

### Recommended Additional Data
For comprehensive benchmarking, consider adding:

**Small** (good for quick iteration):
- < 1 MB file size
- < 1,000 rows
- 5-10 variables

**Medium** (typical use case):
- 10-100 MB file size
- 10,000-100,000 rows
- 10-50 variables

**Large** (stress test):
- > 1 GB file size
- > 1,000,000 rows
- 50+ variables

## Resources

### Documentation
- [Criterion User Guide](https://bheisler.github.io/criterion.rs/book/)
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [Benchmark-Driven Development](https://blog.rust-lang.org/2021/03/18/Rust-1.51.0.html#splitting-debug-information)

### Tools
- [cargo-flamegraph](https://github.com/flamegraph-rs/flamegraph)
- [cargo-benchcmp](https://github.com/BurntSushi/cargo-benchcmp)
- [hyperfine](https://github.com/sharkdp/hyperfine) - CLI benchmarking (see [below](#benchmarking-with-hyperfine))

### Blog Posts
- [How to Write Fast Rust Code](https://deterministic.space/high-performance-rust.html)
- [Performance Analysis Techniques](https://easyperf.net/blog/)
- [Profiling Rust Applications](https://nnethercote.github.io/2022/07/27/how-to-speed-up-the-rust-compiler-in-2022.html)

## Next Steps

1. **Run full benchmark suite**: `cargo bench`
2. **Review HTML reports**: Open `target/criterion/report/index.html`
3. **Identify bottlenecks**: Look for slowest operations
4. **Profile with flamegraph**: Focus on hotspots
5. **Implement optimizations**: Test one at a time
6. **Validate improvements**: Compare against baseline
7. **Document findings**: Update this file with results

## Questions?

- See detailed README: `crates/readstat/benches/README.md`
- Check Criterion docs: https://bheisler.github.io/criterion.rs/book/
- Review performance evaluation: Memory-mapped files analysis (separate doc)

## Benchmarking with hyperfine
Benchmarking performed with [hyperfine](https://github.com/sharkdp/hyperfine).

This example compares the performance of the Rust binary with the performance of the C binary built from the `ReadStat` repository.  In general, hope that performance is fairly close to that of the C binary.

To run, execute the following from within the `readstat` directory.

```powershell
# Windows
hyperfine --warmup 5 "ReadStat_App.exe -f crates\readstat-tests\tests\data\cars.sas7bdat tests\data\cars_c.csv" ".\target\release\readstat.exe data crates\readstat-tests\tests\data\cars.sas7bdat --output crates\readstat-tests\tests\data\cars_rust.csv"
```

:memo: First experiments on Windows are challenging to interpret due to file caching.  Need further research into utilizing the `--prepare` option provided by `hyperfine` on Windows.

```sh
# Linux and macOS
hyperfine --prepare "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches" "readstat -f crates/readstat-tests/tests/data/cars.sas7bdat crates/readstat-tests/tests/data/cars_c.csv" "./target/release/readstat convert crates/readstat-tests/tests/data/cars.sas7bdat --output crates/readstat-tests/tests/data/cars_rust.csv"
```

Other, future, benchmarking may be performed now that [channels and threads](https://github.com/curtisalexander/readstat-rs/issues/28) have been developed.

## Profiling with Flamegraphs
Profiling performed with [cargo flamegraph](https://github.com/flamegraph-rs/flamegraph).

The `readstat` binary lives in the `readstat-cli` crate, so target it with `-p readstat-cli`. Run the following from the repository root.
```sh
cargo flamegraph -p readstat-cli --bin readstat -- data crates/readstat-tests/tests/data/_ahs2019n.sas7bdat --output crates/readstat-tests/tests/data/_ahs2019n.csv
```

Flamegraph is written to `flamegraph.svg` in the directory you run the command from (the repository root).

:memo: Have yet to utilize flamegraphs in order to improve performance.

## Large external SAS7BDAT baseline (Stage 0)

The `large_sas_benchmark` example is an argument-driven baseline for the
current high-level `ReadStatReader` path. It streams batches through `visit`,
counts their rows, and immediately drops them without writing output. Its timed
region includes the reader's metadata parse and all data parses, but excludes
argument parsing and the filesystem metadata lookup.

### Census AHS 2021 corpus

From the repository root, download and extract the public U.S. Census 2021
American Housing Survey National PUF SAS archive into the ignored `target/`
tree (never commit this corpus):

```bash
mkdir -p target/benchmark-data/ahs-2021 && \
curl --fail --location --output target/benchmark-data/ahs-2021/ahs-2021-sas.zip \
  'https://www2.census.gov/programs-surveys/ahs/2021/AHS%202021%20National%20PUF%20v1.0%20SAS.zip' && \
unzip -o target/benchmark-data/ahs-2021/ahs-2021-sas.zip \
  -d target/benchmark-data/ahs-2021
```

The ZIP is approximately 160 MB and contains `household.sas7bdat`
(approximately 311,689,216 bytes), `mortgage.sas7bdat`, `person.sas7bdat`, and
`project.sas7bdat`. Confirm actual sizes with the harness rather than treating
the approximate published sizes as checksums. If extraction creates a nested
directory, find the input with:

```bash
find target/benchmark-data/ahs-2021 -name household.sas7bdat -print
```

### Running and comparing chunk sizes

```bash
cargo run --release -p readstat --example large_sas_benchmark -- \
  target/benchmark-data/ahs-2021/household.sas7bdat --chunk-rows 10000

for rows in 1000 10000 100000; do
  cargo run --quiet --release -p readstat --example large_sas_benchmark -- \
    target/benchmark-data/ahs-2021/household.sas7bdat --chunk-rows "$rows"
done
```

Each run reports source bytes, exact emitted rows and batches, elapsed wall
time, rows/s, source MiB/s, and expected parser invocations. The default
`one-pass` mode uses one metadata parse plus one data parse regardless of batch
count. `legacy-chunked` preserves the former parser-per-batch behavior as a
benchmark baseline; its expected parser count is `1 + batches`. Both counts are
derived rather than instrumented.

Compare the Stage 1 one-pass reader to the former implementation with identical
batch sizing:

```bash
for mode in one-pass legacy-chunked; do
  cargo run --quiet --release -p readstat --example large_sas_benchmark -- \
    target/benchmark-data/ahs-2021/household.sas7bdat \
    --chunk-rows 10000 --mode "$mode"
done
```

On Linux with `/proc` mounted, current RSS is `/proc/self/status` `VmRSS` and
process peak RSS is `VmHWM`, both in KiB. Other platforms and Linux containers
without `/proc` report RSS as unavailable. The process high-water mark includes
runtime allocations and is not solely Arrow batch memory.

For reproducible comparisons:

- Record the commit, release profile, CPU, OS, storage type, and exact command.
- Repeat each size and rotate their order. The harness measures one pass rather
  than providing a statistical framework.
- Label cache state. The first read may be **cold-cache** and storage-bound;
  later reads are normally **warm-cache** due to the OS page cache. Dropping
  Linux caches requires privileges and affects the whole host, so do not do it
  on shared systems.
- Avoid concurrent heavy I/O and CPU work; frequency scaling, thermal
  throttling, and network filesystems can materially affect results.
- MiB/s divides source file bytes by elapsed time. It is workload throughput,
  not measured physical I/O: `legacy-chunked` rereads prefixes, while OS caching
  can serve either mode without issuing storage reads for every source byte.

### Synthetic SAS benchmark corpus

The public Census file is a useful real workload, but its `household.sas7bdat`
member is unusually wide and has only 64,141 rows. The fixed-seed
[`create_rand_ds.sas`](../crates/readstat-tests/util/create_rand_ds.sas) program
defines a complementary canonical profile:

- Dataset: `readstat_benchmark_v1.sas7bdat`
- Seed: `20260727`
- Rows: 4,000,000
- Numeric columns: 12 (SAS numerics occupy 8 bytes each)
- Character columns: 8, each 32 bytes wide
- Compression: none
- Expected raw row payload: 352 bytes, excluding SAS page overhead
- Expected raw payload total: 1,408,000,000 bytes (approximately 1.31 GiB)

This shape is deliberately tall enough to reveal repeated-prefix parsing and
reader partitioning costs. Its high-entropy printable strings also exercise
string conversion and avoid making compression ratio the dominant variable.
The SAS version, host platform, session encoding, and page settings can affect
the random-number implementation or binary representation even with a fixed
seed. The generator writes these details, its parameters, and the complete
`PROC CONTENTS` listing to `$HOME/readstat_benchmark_v1_manifest.txt` in the
same run. On Linux, a SAS session with the `XCMD` option also appends `ls -lh`
and `sha256sum` output through `FILENAME PIPE`. A restricted `NOXCMD` session
records that limitation and the exact fallback commands instead.

The manifest normally records the output size and digest automatically. If SAS
reports `NOXCMD`, run the fallback commands printed in the manifest:

```bash
ls -lh readstat_benchmark_v1.sas7bdat
sha256sum readstat_benchmark_v1.sas7bdat
```

Then validate the file with both benchmark modes and several batch sizes before
publishing it. Exact row counts must agree in every run.

Do **not** commit the generated file or add it to Git LFS. Git LFS charges the
repository owner for stored versions and download bandwidth, making a frequently
downloaded benchmark corpus an unnecessary repository cost. Prefer a dedicated
GitHub release such as `benchmark-data-v1`; GitHub release assets do not consume
Git history and have no aggregate size or bandwidth limit. Each individual
release asset must remain under 2 GiB. If the generated SAS file exceeds that
limit, reduce the canonical row count rather than splitting the file: a split
archive is awkward for automated benchmark setup and obscures the actual source
size.

Publish these alongside the SAS file:

- A SHA-256 checksum file.
- The exact generator program or its repository commit.
- The generated `readstat_benchmark_v1_manifest.txt` file.

The Census and synthetic datasets answer different questions and should both be
retained in benchmark reports; the synthetic corpus must not replace validation
against real files.
