# Project review and parallel pipeline handoff

Status at `2ad4011` (2026-07-28). This document records the decisions from the
correctness/API review and the subsequent reader/writer performance work. It is
not a release changelog; use it to resume design work without reconstructing the
reasoning from commits or Amp threads.

## Original recommendations, by phase

### Phase 1 — correctness and honest contracts (completed)

Highest-priority recommendations were to fix behavior before adding features:

1. Contain Rust panics at every C callback boundary and return typed errors.
2. Treat unknown metadata as unknown. In particular, do not turn ReadStat's
   unknown row-count sentinel into a plausible integer.
3. Use ReadStat's combined missing-value predicate so tagged/user-defined
   missing values become Arrow nulls consistently.
4. Bound Arrow string preallocation. Traditional SAS character variables can
   be 32,767 bytes wide; `rows × declared width × columns` is not a safe
   reservation strategy for large or hostile metadata.
5. Make zero-row, zero-column, visitor-error, byte, mmap, and path behavior
   consistent in the high-level API; remove older tests when focused tests
   replace them.
6. Harden WASM errors and make the excluded WASM package version follow the
   library and CLI release version.

### Phase 2 — one pipeline, bounded memory, measured parallelism (substantially completed)

1. Make `ReadStatReader::visit` the golden path: one parser invocation emits
   ordered bounded batches for path, bytes, and mmap inputs.
2. Feed those batches through a bounded channel so parsing overlaps writing.
3. Retain parallelism only where it improves end-to-end wall time without
   compromising order, errors, transactional publication, or bounded memory.
4. Establish immutable large-file corpora and measure source-only, serial, and
   parallel paths. Do not infer performance from worker counts alone.
5. Remove the old Parquet temporary-file fan-out if it decodes/re-encodes its
   own output or otherwise loses to the serial path.
6. Treat mmap as an input/copy and OS-paging tool, not as row-level random
   access. ReadStat row offsets skip prefixes rather than seeking directly, so
   parser-per-partition work can approach `N × (K + 1) / 2`.

### Phase 3 — API/type simplification and product work (deferred)

- Revisit numeric type narrowing separately; the current proposal is in
  [plan-numeric-type-narrowing.md](plan-numeric-type-narrowing.md).
- Remove the deprecated partitioned reader after a compatibility window,
  preferably in a breaking release.
- Keep SQL in the CLI as an opt-in feature because DataFusion is heavy.
- Produce and version a canonical WASM artifact, then build a local-only SAS
  Explorer (metadata/header visualization, summaries, previews, and possibly
  SQL). GitHub Pages is preferred; a custom subdomain is the fallback.

## Choices made and why

- **One parser is the default reader.** ReadStat cannot seek to an arbitrary SAS
  row; partitioned readers repeatedly scan prefixes and collect too much data.
  The legacy `--parallel` reader is deprecated and retained only for temporary
  compatibility and benchmark comparison.
- **Chunked streaming remains necessary.** It bounds retained Arrow data and
  provides backpressure for enormous SAS files. mmap does not load the whole
  file eagerly, but it does not make the parser itself parallel or seekable.
- **Parallel reads and writes are separate decisions.** The default reader can
  overlap with either a serial or parallel writer.
- **Parquet parallelizes by leaf column within a row group.** Encoded chunks are
  appended once, in schema order, to one transactional output. The measured old
  design (temporary Parquet files, readback, decode, re-encode) was 2.7× slower
  in the orb and was removed.
- **CSV/NDJSON parallelize independent batch encoding.** At most four input
  batches are encoded concurrently and their byte buffers are committed in
  input order. CSV emits one header. Output is byte-identical to serial output.
- **Feather remains serial.** Arrow IPC exposes no supported zero-reencode file
  assembly API, and source-only versus Feather timings show little remaining
  writer headroom.
- **Parallel writing remains opt-in.** It wins on the canonical tall corpus but
  can use substantially more memory on very wide data. Automatic selection
  needs broader machine/workload evidence.
- **Memory bounds are row-based, not byte-based.** A bounded number of very wide
  or string-heavy rows can still be large. For wide data, prefer serial output
  and reduce `--stream-rows`.
- **The benchmark corpus is a GitHub Release asset, not Git LFS.** This avoids
  repository history and LFS storage/bandwidth costs while retaining an
  immutable checksum and manifest.
- **Unknown values are represented explicitly.** `row_count` is optional and
  row-dependent APIs return `RowCountUnavailable`; they do not guess.

## Completed work

### `08b9f2a` — correctness and bounded one-pass streaming

- Added panic containment, checked buffer seeks, combined missing handling,
  unknown row-count handling, bounded string reservations, and focused tests.
- Implemented one-parser bounded `ReadStatReader::visit`; `read` and `chunks`
  remain explicit collecting conveniences.
- Hardened WASM pointer/panic errors and enforced library/CLI/WASM version
  parity in release tooling.
- Added the source-only large-SAS benchmark example and synthetic SAS generator.
- Principal files: `crates/readstat/src/{api,cb,rs_buffer_io,rs_data,rs_metadata}.rs`,
  `crates/readstat-wasm/`, `crates/readstat/examples/large_sas_benchmark.rs`,
  `crates/readstat-tests/tests/convenience_api_test.rs`.

### `e83ca2d`, `93ebf3b` — reproducible benchmark corpus

- Corrected SAS filerefs to the required maximum of eight characters.
- Added generator manifest/version/encoding/`PROC CONTENTS` output, canonical
  ignored drop location, checksum validation, and immutable release publishing.
- Release: `benchmark-data-v1`; canonical file is 1,409,548,288 bytes,
  4,000,000 rows, 20 variables, SHA-256
  `64e39c4ac0a2174cb8d37555f5bd47dba837db083873c609e9bed985d30cbf5b`.
- Principal files: `crates/readstat-tests/util/create_rand_ds.sas`,
  `benchmark-data/README.md`, `scripts/publish-benchmark.sh`.

### `4875472` — canonical CLI pipeline and native parallel Parquet

- Default conversion now uses one reader parser plus a bounded channel and
  incremental writer; reader threads are cancelled/joined on all writer paths.
- Added native ordered Parquet column encoding, transactional staging cleanup,
  empty-output handling, schema/order/value tests, and metadata caching.
- Removed temporary-file merge/re-encode APIs, tests, option, benchmark, and
  production `tempfile` dependency.
- Added `scripts/benchmark-conversion.sh` for machine capture, corpus download,
  checksum verification, release build, Hyperfine comparisons, RSS measurement,
  worker sweep, and input-batch sweep.
- Principal files: `crates/readstat-cli/src/run/convert.rs`,
  `crates/readstat/src/{api,rs_write}.rs`, parallel writer tests, and
  `docs/{ARCHITECTURE,BENCHMARKING,USAGE}.md`.

### `2ad4011` — parallel CSV and NDJSON

- Added transactional `ParallelTextWriter`, bounded four-batch Rayon encoding,
  ordered commit, one CSV header, writer poisoning after partial I/O failure,
  and CLI routing for file output.
- Deprecated `--parallel` in help/docs. CSV stdout and Feather remain serial.
- Added byte-equivalence, row-count, overwrite/race, schema-error, empty-output,
  and staging-cleanup coverage.
- Principal files: `crates/readstat/src/rs_write.rs`,
  `crates/readstat-cli/src/{cli.rs,run/convert.rs}`, and
  `crates/readstat-tests/tests/parallel_write_cli_test.rs`.

## Benchmark conclusions

Canonical 4M-row corpus on an 18-core Apple Silicon Mac with 64 GiB:

| Path | Serial | Parallel | Result |
|---|---:|---:|---|
| Parquet | 1.966 s | 1.511 s | 1.30× faster; RSS 553 → 201 MB |
| CSV | 3.393 s | 1.551 s | 2.19× faster; RSS 61 → 90 MB |
| NDJSON | 3.247 s | 1.564 s | 2.08× faster; RSS 62 → 99 MB |

- Four writer workers saturated all measured formats; more workers did not
  materially help. Parquet results from 4–18 workers were within about 2%.
- Input batches from 10k–100k rows were effectively tied; 5k was 14% slower.
- The one-pass reader was 2.36× faster than deprecated partitioned reading.
- CSV and NDJSON serial/parallel outputs had identical lengths, SHA-256 values,
  and `cmp` results.
- Wide AHS data (1,078 columns) is the counterexample: Parquet serial RSS was
  757 MB, versus 1.38 GB parallel at 100k-row groups and 1.15 GB at 25k. Smaller
  groups also increased output size by 6–7%. Keep the 100k default and document
  serial/smaller-input-batch guidance rather than claiming a universal win.

## Recommended next phase

Finish **Phase 2 stabilization before starting numeric narrowing**:

1. Run the conversion benchmark on representative 4/8-core Linux and Windows
   hosts. Limiting Rayon on an 18-core Mac is not equivalent hardware evidence.
2. Keep `--parallel-write` explicit until those results support an adaptive
   policy. If automation is pursued, include width/estimated-byte pressure, not
   CPU count alone.
3. Decide and document the release in which deprecated `--parallel` will be
   removed; do not invest further in its parser-per-partition architecture.
4. Add a disk-full/fault-injection test if practical for the public text-writer
   poisoning contract.
5. Then review `plan-numeric-type-narrowing.md` as a separate behavior change;
   do not mix it with pipeline refactoring.

After that, improve WASM artifact release/error reporting and prototype the SAS
Explorer. SQL remains opt-in; parallel SQL output is deferred and currently
incompatible with `--parallel-write`.

## Validation run

For `4875472`:

- `cargo test --workspace --no-default-features` passed (144 library tests,
  integration/CLI tests, and 13 doctests; one benchmark test ignored).
- Focused parallel writer tests: 3 passed; parallel CLI tests: 5 passed;
  convenience reader tests: 10 passed.
- `cargo clippy -p readstat --all-targets --no-default-features --features csv,parquet,feather,ndjson -- -D warnings` passed.
- `cargo clippy -p readstat-cli --all-targets -- -D warnings` passed.
- `cargo fmt --all`, release build, Bash syntax check, and `git diff --check`
  passed.

For `2ad4011`:

- `cargo test --workspace` passed (150 library tests, all CLI/integration tests,
  and 13 doctests).
- `cargo clippy --workspace --all-targets -- -D warnings` passed.
- CSV-only, NDJSON-only, and no-default-feature builds passed.
- Release serial/parallel comparisons and byte-for-byte validation passed.

Known non-product failures/risks:

- One earlier full-feature workspace attempt in the constrained orb exhausted
  disk while linking DataFusion test binaries; focused and no-default suites
  passed after deleting generated `target/debug` artifacts. This was an
  environment-capacity failure, not a test assertion failure.
- One investigative `ndjson-null` Hyperfine warmup exited nonzero; final NDJSON
  file benchmarks and the complete workspace suite passed.
- Path-backed `ReadStatReader` metadata is cached, not a stable file snapshot;
  callers must not replace the source between metadata and data parses.
- Transactional publication can report an error after destination hard-link
  creation if staging unlink fails (pre-existing edge case).
- Positive-row/zero-column Parquet output is rejected because Parquet cannot
  represent its row count without inventing a column.
