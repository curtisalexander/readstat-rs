# Performance Benchmarking with Criterion

## Overview

This document provides a comprehensive guide to performance benchmarking in readstat-rs using [Criterion.rs](https://github.com/bheisler/criterion.rs).

## Quick Start

```bash
# Run all benchmarks
cd crates/readstat
cargo bench

# View HTML reports
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
- **all_types.sas7bdat** - 3 rows, 8 vars (tiny)
- **cars.sas7bdat** - 428 rows, 15 vars (small)

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
- [hyperfine](https://github.com/sharkdp/hyperfine) - CLI benchmarking

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
