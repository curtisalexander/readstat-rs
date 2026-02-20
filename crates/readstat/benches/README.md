# ReadStat Benchmarks

This directory contains comprehensive performance benchmarks using [Criterion.rs](https://github.com/bheisler/criterion.rs).

## Overview

The benchmarks measure performance across key operations:

1. **Metadata Reading** - Parsing file headers and variable metadata
2. **Data Reading** - Reading SAS7BDAT data in various chunk sizes
3. **Arrow Conversion** - Converting SAS data to Arrow RecordBatch format
4. **Writing** - Output performance for CSV, Parquet, Feather, and NDJSON
5. **Compression** - Parquet compression algorithm comparison
6. **Buffer Sizes** - SpooledTempFile buffer size optimization
7. **End-to-End** - Complete read + write pipeline

## Running Benchmarks

### Run All Benchmarks
```bash
cd crates/readstat
cargo bench
```

### Run Specific Benchmark Group
```bash
# Metadata reading only
cargo bench metadata_reading

# Data reading benchmarks
cargo bench read_single_chunk
cargo bench read_chunked

# Writing benchmarks
cargo bench write_csv
cargo bench write_parquet_compression
cargo bench write_formats

# Parallel write buffer sizes
cargo bench parallel_write_buffer_sizes

# End-to-end conversion
cargo bench end_to_end_conversion
```

### Run Benchmark for Specific Parameter
```bash
# Test only CSV writing
cargo bench write_formats/csv

# Test specific chunk size
cargo bench read_chunked/chunk_size/10000

# Test specific compression
cargo bench write_parquet_compression/snappy
```

## Viewing Results

### HTML Reports
After running benchmarks, view detailed HTML reports:
```bash
open target/criterion/report/index.html  # macOS
xdg-open target/criterion/report/index.html  # Linux
start target/criterion/report/index.html  # Windows
```

### Command Line Output
Criterion provides:
- **Time per iteration** (mean, median, std dev)
- **Throughput** (elements/sec, MB/sec)
- **Comparison to baseline** (% change from previous runs)
- **Statistical significance** (outlier detection)

### Example Output
```
metadata_reading/all_types.sas7bdat
                        time:   [1.2345 ms 1.2567 ms 1.2789 ms]
                        thrpt:  [812.34 Kelem/s 825.67 Kelem/s 838.90 Kelem/s]
                        change: [-2.3456% -1.2345% +0.1234%] (p = 0.12 > 0.05)
                        No change in performance detected.
```

## Benchmark Groups Explained

### 1. Metadata Reading (`metadata_reading`)
**Purpose:** Measure overhead of parsing file structure without reading data.

**Tests:**
- `all_types.sas7bdat` - Small file with diverse types
- `cars.sas7bdat` - Typical dataset

**What to optimize:**
- If this is slow: Optimize C library callback overhead or metadata struct allocation

### 2. Single Chunk Reading (`read_single_chunk`)
**Purpose:** Measure raw data reading performance for entire datasets.

**Throughput:** Reports rows/second

**What to optimize:**
- If slow: Bottleneck is likely in ReadStat C library or FFI overhead
- Memory-mapped files could help here if I/O bound

### 3. Chunked Reading (`read_chunked`)
**Purpose:** Compare performance of different streaming chunk sizes.

**Tests chunk sizes:** 1,000 / 5,000 / 10,000 rows

**What to look for:**
- **Smaller chunks:** More overhead from repeated parsing setup
- **Larger chunks:** Higher memory usage but potentially faster
- **Sweet spot:** Balance between memory and performance (currently 10,000)

**What to optimize:**
- Adjust default `STREAM_ROWS` constant if benchmarks show different optimal size

### 4. Arrow Conversion (`arrow_conversion`)
**Purpose:** Measure overhead of converting SAS types to Arrow format.

**What to optimize:**
- If slow: Arrow builder allocation or type conversion logic
- Consider pre-allocating builders with capacity

### 5. CSV Writing (`write_csv`)
**Purpose:** Baseline for write performance.

**What to optimize:**
- BufWriter buffer size (currently 8KB default)
- CSV formatting overhead

### 6. Parquet Compression (`write_parquet_compression`)
**Purpose:** Compare compression algorithms and their CPU/size trade-offs.

**Tests:**
- `uncompressed` - Fastest write, largest file
- `snappy` - Balanced (default)
- `zstd` - Slower write, best compression

**What to look for:**
- Time vs compression ratio trade-off
- CPU usage patterns

**What to optimize:**
- Choose default compression based on typical use case
- Consider compression level tuning

### 7. Parallel Write Buffer Sizes (`parallel_write_buffer_sizes`)
**Purpose:** Find optimal SpooledTempFile buffer size.

**Tests:** 1 MB / 10 MB / 100 MB / 500 MB buffers

**What to look for:**
- **Small buffers:** More disk I/O, potentially slower
- **Large buffers:** Better performance but higher memory
- **Inflection point:** Where performance plateaus

**What to optimize:**
- Adjust default `parallel_write_buffer_mb` (currently 100 MB)
- Document recommended values for different scenarios

### 8. Write Formats (`write_formats`)
**Purpose:** Compare output format performance.

**Tests:** CSV / Parquet / Feather / NDJSON

**What to look for:**
- Relative performance of each format
- Format selection guidance

**What to optimize:**
- Per-format specific optimizations
- Buffer sizes for each writer

### 9. End-to-End Conversion (`end_to_end_conversion`)
**Purpose:** Real-world performance including both read and write.

**Most important benchmark** - represents actual usage

**What to look for:**
- Total pipeline latency
- Bottleneck identification (read vs write)

**What to optimize:**
- Overall architecture if this is slow
- Pipeline efficiency

## Interpreting Results

### Statistical Significance
Criterion uses statistical analysis to determine if changes are meaningful:
- **p < 0.05**: Change is statistically significant
- **p ≥ 0.05**: Change might be noise

### Throughput Metrics
- **Elements/sec**: Rows processed per second
- **Higher is better**
- Compare across different parameters

### Variance
- **Low variance** (< 5%): Consistent performance
- **High variance** (> 10%): May indicate:
  - System background activity
  - GC pressure
  - Cache effects
  - Need more samples

## Optimization Workflow

### 1. Establish Baseline
```bash
cargo bench --save-baseline main
```

### 2. Make Changes
Edit code with potential optimization

### 3. Compare Performance
```bash
cargo bench --baseline main
```

### 4. Analyze Results
Look for:
- ✅ Significant improvements (> 10%, p < 0.05)
- ❌ Regressions (performance decreased)
- ⚠️ High variance (results unreliable)

### 5. Profile Bottlenecks
If optimization didn't help:
```bash
cargo flamegraph --bench readstat_benchmarks -- --bench
```

## Best Practices

### Before Benchmarking
1. **Close background applications**
2. **Disable CPU frequency scaling** (if possible):
   ```bash
   # Linux
   sudo cpupower frequency-set --governor performance
   ```
3. **Run on battery power** (laptops may throttle on AC)
4. **Ensure consistent system state**

### During Benchmarking
1. **Don't interact with system** - let it run
2. **Multiple runs** - Criterion handles this automatically
3. **Warm caches** - Criterion does warm-up iterations

### After Benchmarking
1. **Check variance** - High variance = unreliable results
2. **Compare to baseline** - Use `--baseline` flag
3. **Profile if needed** - Flamegraphs show hotspots
4. **Document findings** - Update optimization notes

## Common Performance Patterns

### CPU-Bound Workloads
- Reading (parsing SAS format)
- Compression (zstd, brotli)
- Arrow conversion

**Optimization strategies:**
- Parallel processing (already implemented)
- SIMD operations
- Algorithm improvements

### I/O-Bound Workloads
- Large file reading
- Writing uncompressed formats

**Optimization strategies:**
- Larger buffers (BufWriter, SpooledTempFile)
- Memory-mapped files (for reads)
- Async I/O (future consideration)

### Memory-Bound Workloads
- Large chunk sizes
- Multiple parallel streams

**Optimization strategies:**
- Optimize chunk size
- Bounded channels (already implemented)
- Stream processing

## Benchmark Data Files

Tests use files from `crates/readstat-tests/tests/data/`:
- **all_types.sas7bdat** - 3 rows, 10 variables, diverse types
- **cars.sas7bdat** - 1081 rows, 13 variables, typical dataset

For production benchmarking, consider adding:
- **Small file** (< 1 MB, < 1K rows)
- **Medium file** (10-100 MB, 10K-100K rows)
- **Large file** (> 1 GB, > 1M rows)

## CI Integration

### GitHub Actions
Add benchmark check to CI (runs but doesn't fail):
```yaml
- name: Run benchmarks (no fail)
  run: cargo bench --no-fail-fast
  continue-on-error: true
```

### Performance Regression Detection
Use [Criterion Action](https://github.com/boa-dev/criterion-compare-action):
```yaml
- uses: boa-dev/criterion-compare-action@v3
  with:
    branchName: main
```

## Future Enhancements

### Additional Benchmarks
- [ ] Parallel vs sequential reading comparison
- [ ] Network file system performance
- [ ] Compressed SAS files (if supported)
- [ ] Memory usage profiling (with dhat)
- [ ] Different file sizes (small/medium/large)

### Advanced Profiling
- [ ] CPU profiling with flamegraphs
- [ ] Memory profiling with valgrind/heaptrack
- [ ] Cache profiling with perf
- [ ] System call tracing with strace

### Platform Comparison
- [ ] Linux vs macOS vs Windows
- [ ] Different CPU architectures (x86 vs ARM)
- [ ] Different storage types (SSD vs HDD vs NFS)

## Resources

- [Criterion.rs User Guide](https://bheisler.github.io/criterion.rs/book/)
- [Benchmarking in Rust](https://easyperf.net/blog/2022/05/28/Performance-analysis-and-tuning-contest-8)
- [The Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [Profiling Rust Programs](https://nnethercote.github.io/2022/07/27/how-to-speed-up-the-rust-compiler-in-2022.html)
