# crates.io Release-Prep Review

Deep pre-release review of the entire repo (2026-06-09), covering the FFI core, public
API, writers/CLI, packaging, build system, and documentation. Every "critical" claim
below was independently verified before inclusion. Baseline health: `cargo clippy
--workspace --all-targets --all-features` is clean and the full test suite (including
all 12 doctests) passes.

Overall: the repo is far more polished than a typical first release — curated
re-exports, `#[non_exhaustive]` error enum, docs.rs metadata, pre-generated bindings
tested on 5 targets in CI, and all crate-level doc examples are signature-correct.
The findings below are what remains.

Good news on names: `readstat`, `readstat-sys`, `readstat-cli`, and
`readstat-iconv-sys` are **all unregistered** on crates.io — nothing squatted.

**Progress tracking:** each numbered item carries a status line:
`**Status:** ☐ not started` / `🔄 in progress` / `✅ done (date — how verified)` /
`⏭️ skipped (reason)`. Update as we go.

---

## 1. Release blockers (verified broken right now)

### 1.1 `include = [...]` in both sys-crate manifests is dead TOML
**Status:** ✅ done (2026-06-10 — `include` moved into `[package]` in both manifests;
`cargo package --list`: readstat-sys 230→107 files / 1.46 MB, iconv-sys 1,696→324
files / 6.1 MB; LICENSE added to both lists and crate dirs)
In `crates/readstat-sys/Cargo.toml` and `crates/readstat-iconv-sys/Cargo.toml`, the
`include` array sits *after* the `[[package.metadata.release.pre-release-replacements]]`
table header, so TOML assigns it to that table — `package.include` is `None`
(verified with a TOML parser).

**Consequence:** `cargo package` grabs everything — `readstat-iconv-sys` packs
**1,696 files / 26.5 MB** including a 5.7 MB tarball
(`vendor/libiconv-win-build/distfiles/libiconv-1.18.tar.gz`) and four Visual Studio
build trees — very likely over the 10 MB crates.io limit. `readstat-sys` ships 230
files / 1.96 MB including `.github/`, `VS17/`, `fuzz/dict/`, `man/`.

**Fix:** move `include` up into the `[package]` section in both files. With that
fixed, the lists themselves are complete — every C file and transitive header
`build.rs` needs (including `CKHashTable.h`) was traced and is covered. Expected
package sizes after the fix: readstat-sys ≈ 1.3 MB, readstat-iconv-sys ≈ 6.6 MB
uncompressed — comfortably under the limit.

### 1.2 `scripts/vendor.sh prepare` is broken — and destructively
**Status:** ✅ done (2026-06-10 — progress echoes redirected to stderr; added a
guard that aborts before deleting submodules unless both temp copies verifiably
exist; full prepare→status→restore round-trip tested in a throwaway clone)
`copy_readstat_files`/`copy_iconv_files` echo progress lines to stdout
("Copying ReadStat vendor files...", "  Copied N files") while the caller captures
stdout with `$(...)`, so the captured "path" is multi-line garbage. Under
`set -euo pipefail` the script dies *after* it has already `rm -rf`'d both vendor
directories (recoverable via `restore`, but the documented Linux/macOS publish path
cannot succeed). `vendor.ps1` is correct.

**Fix:** send progress echoes to stderr (`>&2`) or have the functions output only
the path.

### 1.3 `--no-default-features --features sql` does not compile
**Status:** ✅ done (2026-06-10 — `sql` now enables the four format features;
verified `cargo check` passes for `--features sql`, bare `--no-default-features`,
and each format feature standalone; silenced the no-features dead-code warning on
`ReadStatWriter`. CI powerset check still TODO — see pre-publish checklist)
`crates/readstat/src/rs_query.rs:8-19` unconditionally uses `parquet`, `arrow_csv`,
`arrow_ipc`, `arrow_json` (and calls `resolve_parquet_compression`, which is
`#[cfg(feature = "parquet")]`), but `sql = ["dep:datafusion", "dep:tokio",
"dep:futures"]` doesn't activate them. Reproduced:
`cargo check -p readstat --no-default-features --features sql` fails with 7 errors.

**Fix:** add the format features to `sql = [...]` or cfg-gate the format arms in
`write_sql_results`. Add `cargo hack check --feature-powerset` to CI to prevent
regression.

### 1.4 Declared MSRV 1.85 is wrong
**Status:** ✅ done (2026-06-10 — `rust-version = "1.88"` in workspace +
readstat-wasm; README.md and docs/BUILDING.md updated; verified
`cargo +1.88 check --workspace --all-features` passes; no dependency in the graph
requires >1.87)
Let-chains (`if x && let Some(y) = ...`) at `crates/readstat-sys/build.rs:93,98`,
`crates/readstat/src/rs_write.rs:535`, and `crates/readstat-cli/src/run.rs:583`
require Rust **1.88** (stabilized with edition 2024 let-chains). A 1.85 user passes
cargo's MSRV resolution check, then hits a compile error.

**Fix:** set `rust-version = "1.88"` in the workspace; ideally verify with
`cargo msrv verify`. Re-check arrow v58 / datafusion v53 MSRVs at the same time.

### 1.5 The fuzz targets don't compile against the current library
**Status:** ✅ done (2026-06-10 — removed both `set_no_progress` calls; fixed the
misleading "library itself is fine" comment; `cargo +nightly check` passes and all
three targets ran their seed corpora clean via `cargo fuzz run <t> -- -runs=0`)
`fuzz/fuzz_targets/fuzz_read_data.rs:21` and
`fuzz/fuzz_targets/fuzz_read_data_filtered.rs:80` call
`ReadStatData::set_no_progress(true)`, which was removed (current API is
`set_progress`). The "fuzzed" claim in SECURITY/README is stale until this is fixed
and the corpora re-run.

**Fix:** delete the calls (no progress callback is attached by default), re-run the
corpora before release.

---

## 2. Correctness bugs in the code (high priority)

### 2.1 Reader-thread errors are swallowed → corrupt output with exit code 0
**Status:** ✅ done (2026-06-10 — reader thread now returns `Err` on the first
chunk error instead of `eprintln!`+`Ok`; `join_reader` is checked after the
channel drains and BEFORE finalizing output in all three write branches
(sequential/parallel-write/SQL); `finish()` is called unconditionally once the
channel drains rather than gated on a chunk index. New integration test
`cli_robustness_test::truncated_input_exits_nonzero` confirms nonzero exit + no
leftover file)
`crates/readstat-cli/src/run.rs:447-471`: per-chunk parse errors are `eprintln!`'d
but the reader thread returns `Ok(())`, so `run_data` returns `Ok` and the process
exits 0. Worse, the sequential write loop (`run.rs:608-613`) only calls
`wtr.finish()` when `i == pairs_cnt - 1`; with a chunk missing, the channel closes
before that index is reached, so the Parquet/Feather footer is never written — an
**unreadable file**, a success message, and exit 0. For CSV/NDJSON the failed chunk
is silently omitted from the middle of the output.

**Fix:** have the reader thread return `Err` (first error or joined summary) so it
propagates; call `finish()` unconditionally after the channel drains; only print the
success summary when the reader reported no errors; ideally delete partial output on
failure. *This is the single most important code fix.*

### 2.2 Sub-second datetime values truncate instead of round
**Status:** ✅ done (2026-06-10 — added `sas_datetime_to_unix_subsec`/`sas_time_to_us`
helpers that `.round()` before the checked cast; all four sub-second arms (ms/µs/ns
datetime + µs time) now route through them. Six unit tests in
`cb::tests::subsecond_conversion`, including a 1000-fraction sweep that the old
truncation failed for ~half of)
`crates/readstat/src/cb.rs:482, 492, 502, 522` (via `checked_f64_to_i64` at
`cb.rs:252`): `(val - SEC_SHIFT) * 1000.0` followed by an `as`-cast truncation.
At modern SAS-datetime magnitudes (~1.9e9 s) the f64 representation error (~1e-7)
dwarfs the rounding granularity, so roughly half of all fractional-second values come
out one ms/µs/ns low (e.g. `…800.123` → `…122` ms). `round_decimal_f64` does not
help at these magnitudes.

**Fix:** apply `.round()` to the scaled product before the checked cast in all four
sub-second arms. Document (or also round) floor-vs-trunc behavior for pre-1960
values in the second-precision arms. Add a fractional-seconds round-trip test.

### 2.3 Two panic paths inside the `extern "C"` value callback
**Status:** ✅ done (2026-06-10 — `d.builders[col_index]` → `d.builders.get_mut(...)`
with a `type_mismatch_abort!` on `None`; string arm `as_string_mut()` →
`let ColumnBuilder::Str(sb) = builder else { type_mismatch_abort!() }`; removed the
now-unused panicking `as_string_mut` helper. No panic path remains in the callback)
`crates/readstat/src/cb.rs:337` (`&mut d.builders[col_index as usize]` — unchecked
index) and `cb.rs:364` (`as_string_mut()` — panics on non-string builder). A panic
in `extern "C"` aborts the process (Rust ≥ 1.81). Both are reachable in safe code
via metadata/data mismatch — e.g. file modified on disk between the metadata parse
and the data parse (they are separate file opens), or `init(md_A)` + `read_data(B)`.
Every other mismatch path uses the graceful `type_mismatch_abort!` macro.

**Fix:** use `d.builders.get_mut(...)` with the abort macro on `None`; replace
`as_string_mut()` with `if let ColumnBuilder::Str(b)` + `type_mismatch_abort!()`.

### 2.4 "Streaming" mode materializes the whole file in memory
**Status:** ✅ done (2026-06-10 — fixed as part of the §2.1 rewrite: the
non-parallel path now parses and `s.send()`s one chunk at a time, so the bounded
channel provides real backpressure (~CHANNEL_CAPACITY chunks resident regardless
of file size). `--parallel` still buffers by design (order must be preserved) and
is now documented as a memory/speed tradeoff. Verified seq/parallel/streaming
produce byte-identical output. Note: the SQL path still collects all batches —
inherent to the MemTable query model; folding it onto the streaming SQL APIs is
tracked in §5)
`crates/readstat-cli/src/run.rs:412-445`: `par_iter().map(...).collect()` collects
**every** chunk's `ReadStatData`/`RecordBatch` into a `Vec` before sending anything
on the channel — for both `--parallel` and the default single-thread pool. The
bounded(10) channel and the documented memory model ("streams rows in chunks (10k)
to manage memory") are defeated; a larger-than-RAM file OOMs in the mode built to
prevent that.

**Fix:** in the non-parallel case, iterate offset pairs in a plain loop and send each
chunk as produced; in the parallel case, send from within the rayon tasks (chunks
carry their index for ordering) or use a bounded work queue.

### 2.5 CSV/NDJSON writers are never flushed; flush errors discarded
**Status:** ✅ done (2026-06-10 — added `flush_buffered()` and call it from
`finish()` for Csv/Ndjson (and CsvStdout) before printing the summary, so a flush
I/O error surfaces instead of being swallowed by `BufWriter::drop`)
`crates/readstat/src/rs_write.rs:268-284`: `finish()` for CSV/NDJSON only prints the
summary; the `BufWriter<File>` flushes in `Drop`, where I/O errors are discarded.
On ENOSPC: truncated file, success message naming the full row count, exit 0.
(Parquet/Feather are safe — their `close()/finish()` flush; verified against
arrow/parquet 58.1.0 source.)

**Fix:** explicit `flush()?` in `finish()` for Csv and Ndjson (for CSV also flush the
arrow writer via `into_inner()` per batch rather than relying on csv's silent Drop);
print the summary only after a successful flush.

### 2.6 Zero rows ⇒ no output file is created at all
**Status:** ✅ done (2026-06-10 — added `write_empty_output` helper (CLI) that
writes one empty `RecordBatch` when the channel yields nothing; wired into both
the sequential and parallel-write branches. SQL: `execute_sql_async` now returns a
single empty batch carrying the result schema instead of `Ok(vec![])`. Three new
integration tests verify valid header-only CSV / `PAR1`-bracketed Parquet / `ARROW1`
Feather for `--rows 0`)
`crates/readstat-cli/src/run.rs:368` + `crates/readstat/src/common.rs:22-32`:
`build_offsets(0, n)` yields zero window pairs, so neither `write()` nor `finish()`
runs — no output file, yet "Writing parsed data to file …" prints and exit is 0.
Same in the SQL path: `write_sql_results` (`rs_query.rs:246-248`) returns early on
empty batches, so `WHERE 1=0` produces no file.

**Fix:** when the channel yields nothing, write one
`RecordBatch::new_empty(schema)` before finishing; pass the result schema into
`write_sql_results` and do the same.

### 2.7 Builder pre-allocation trusts file-claimed row counts (OOM DoS)
**Status:** ✅ done (2026-06-10 — added `MAX_PREALLOC_ROWS = 1_000_000`; capacity
clamped via `.min(MAX_PREALLOC_ROWS)` in `allocate_builders`; string byte hint uses
`saturating_mul`; `set_chunk_counts` uses `saturating_sub` to avoid underflow on
`row_end < row_start`. Fixed the misleading fuzz comment. Builders still grow on
demand, so honest files are unaffected)
`crates/readstat/src/rs_data.rs:121-126, 261-269`: `allocate_builders` reserves
`capacity` rows per column and `capacity * storage_width` bytes per string column,
where both values come unvalidated from the file header. A crafted header claiming
2^31 rows × large widths triggers multi-GB allocation (or a debug-build multiply
overflow panic) before a single row is parsed. The fuzz targets cap this externally;
the library's own `read_to_batch` does not — and the comment in
`fuzz/fuzz_targets/fuzz_read_data.rs:6-8` misstates this ("The library itself is
fine"). Related: `init(md, row_start, row_end)` with `row_end < row_start`
underflows at `rs_data.rs:490-491` and feeds the same pre-allocation.

**Fix:** clamp the capacity hint (`capacity.min(MAX_PREALLOC_ROWS)` and
`saturating_mul` with a byte ceiling) — builders grow dynamically, so this is free
for honest files. Use `saturating_sub` (or validate) in `set_chunk_counts`. Fix the
fuzz comment.

### 2.8 Cross-compiling to Windows is broken (host-vs-target cfg bug)
**Status:** ✅ done (2026-06-10 — `readstat-iconv-sys/build.rs` now has a single
unconditional `main()` that early-returns unless `CARGO_CFG_TARGET_OS == "windows"`,
replacing the host-evaluated `#[cfg(windows)]`. Verified default + `buildtime_bindgen`
builds still compile on a macOS host; `lib.rs`'s `#[cfg(windows)]` include is
target-evaluated and unchanged)
`crates/readstat-iconv-sys/build.rs:15, 115`: `#[cfg(windows)] fn main()` gates on
the **host** (build scripts compile for the host), so cross-compiling from
Linux/macOS to `x86_64-pc-windows-msvc` (cargo-xwin) gets the no-op build script: no
iconv compiled, no `cargo:include`, no `OUT_DIR/bindings.rs` → cryptic failure.
Reverse case: a Windows host targeting non-Windows runs the full iconv compile
pointlessly.

**Fix:** single unconditional `main()` that early-returns unless
`env::var("CARGO_CFG_TARGET_OS")? == "windows"`.

### 2.9 macOS x86_64 release artifact is actually arm64
**Status:** ✅ done (2026-06-10 — both macOS release jobs in `main.yml` now build
with explicit `--target ${{ env.target }}` and use `target/<triple>/release/` paths,
so the x86_64 artifact is a genuine x86_64 binary rather than a cross-labeled arm64
one. Cannot run the GH runner locally; change verified by inspection against the
linux-aarch64 job's working pattern)
`.github/workflows/main.yml`, `build-macos-x86` job (~line 298-340): runs on
`macos-latest` (an arm64 runner since GitHub's migration), installs the
`x86_64-apple-darwin` target, but builds with plain `cargo build --release` — no
`--target` — and tars the result as `x86_64-apple-darwin`.

**Fix:** run on `macos-15-intel` (as `readstat-sys-ci.yml` already does) or add
`--target ${{ env.target }}` and adjust artifact paths.

---

## 3. API/semver decisions (cheap now, breaking later)

**Status:** ✅ done (2026-06-10). Decisions taken: error variants kept
SCREAMING_SNAKE (no rename); library stdout printing refactored out; pub fields
locked down; dedicated error variants added. `cargo clippy
--workspace --all-targets --all-features` clean; full `cargo test --workspace
--all-features` green (incl. 12 doctests); each feature subset
(`--no-default-features` + sql/csv/parquet) compiles; CLI smoke-tested
(metadata + "wrote N rows" summary render correctly). What landed:
- **`ReadStatCError`**: `#[non_exhaustive]` + `Clone, Copy, PartialEq, Eq` +
  `impl std::error::Error`. Variants kept matching the C names (per decision).
- **`#[non_exhaustive]`** added to `ReadStatCompress`, `ReadStatEndian`,
  `ReadStatVarType`, `ReadStatVarTypeClass` (now consistent with the others).
- **Arrow re-exported**: `pub use arrow / arrow_array / arrow_schema` + a crate
  doc "Arrow version policy" section (pinned v58, major bump = semver-major).
  `crossbeam` re-exported too.
- **`ChunkReceiver`** made `pub` (+ re-exported) so the streaming SQL API's
  receiver type is nameable.
- **Pub-field lockdown**: `ReadStatPath.cstring_path` → `pub(crate)`;
  `.extension` removed entirely (was dead plumbing, derivable from `path`).
  `WriteConfig` fields → `pub(crate)` with `out_path()/format()/overwrite()/
  compression()/compression_level()` accessors. `ReadStatMetadata.is_64bit`
  `i32` → `bool` (JSON now emits `true`/`false`; ~18 test files updated).
- **Library stdout removed**: `finish()` now returns rows written (dropped the
  display-only `in_path` param); `write_metadata*` replaced by
  `metadata_to_string` / `metadata_to_json` / `format_metadata` returning
  `String`. CLI owns all printing (`print_write_summary`, `format_with_commas`
  moved to the CLI). Metadata display also collapsed the duplicate "arrow
  logical/physical" line into one "arrow data type" (§5 nit folded in).
- **Error granularity**: added `FileNotFound`, `UnsupportedInputExtension`,
  `OutputExtensionMismatch`, `OutputFileExists`, `OutputParentMissing`,
  `UnknownFormat`, `EmptySqlFile` and routed `rs_path`/`rs_write_config`/
  `rs_query`/`OutFormat::from_str` through them. CLI-flag wording removed from
  library messages ("set overwrite = true" instead of "--overwrite").
- **Misc**: `ParquetCompression` gained `FromStr` + `PartialEq/Eq/Hash`;
  `write_batch_to_parquet`/`merge_parquet_files` marked `#[doc(hidden)]`;
  `.sas7bcat` now rejected up front (catalogs aren't parseable).

Original notes (for reference):

One-time chances — after first publish each costs a major bump.

- **`ReadStatCError`** (`err.rs:16`): add `#[non_exhaustive]` (the C library does
  add codes — e.g. `BAD_MR_STRING` is recent), derive `Clone, Copy, PartialEq, Eq`,
  and `impl std::error::Error`. Consider renaming the 41 `READSTAT_ERROR_*`
  SCREAMING_SNAKE variants to idiomatic Rust names — never cheaper than now. Also:
  `READSTAT_OK` is a variant of an *error* enum (can never escape, but confusing in
  docs).
- **`#[non_exhaustive]` inconsistently applied**: `ReadStatVarFormatClass`,
  `OutFormat`, `ParquetCompression`, `ReadStatError` have it; `ReadStatCompress`,
  `ReadStatEndian` (`rs_metadata.rs:380, 393`), `ReadStatVarType`,
  `ReadStatVarTypeClass` (`rs_var.rs:51, 73`) don't, despite also mirroring C enums.
- **Re-export Arrow**: `pub use arrow; pub use arrow_array; pub use arrow_schema;`
  and state the Arrow major-version policy in crate docs/README. Without this every
  consumer pins Arrow 58 themselves and gets "expected RecordBatch, found
  RecordBatch" errors on mismatch; every Arrow bump is a silent breaking change.
- **Private type in public signatures**: `execute_sql_stream` /
  `execute_sql_and_write_stream` (`rs_query.rs:129, 174`) take `ChunkReceiver`, a
  *private* alias for `crossbeam::channel::Receiver<(ReadStatData, ReadStatPath,
  usize)>`; crossbeam isn't re-exported, so users can't name the type. Make the
  alias public + re-export the channel type, or wrap in a newtype.
- **Pub-field audit**: `ReadStatPath.cstring_path: CString` and `extension` are FFI
  plumbing — make private (`pub(crate)`). `ReadStatMetadata.is_64bit: i32` should be
  `bool`; its `Serialize` derive freezes the JSON shape. `WriteConfig` pub fields
  bypass all of `new()`'s validation. `ReadStatData.batch: Option<RecordBatch>` as
  the only result channel — consider `take_batch()`. Decide each deliberately.
- **Library prints to stdout**: `ReadStatWriter::finish` unconditionally `println!`s
  "In total, wrote N rows…" (`rs_write.rs:306-324`); `write()` silently writes CSV
  to stdout when `out_path` is None (`rs_write.rs:367-377`); `write_metadata*`
  (`rs_write.rs:597-670`) are stdout-printers in the library. Return the row count
  and let the CLI print.
- **Error granularity**: file-not-found (`rs_path.rs:96`), wrong extension
  (`rs_path.rs:73-85`), output-exists (`rs_write_config.rs:224`), unknown format
  (`rs_write_config.rs:64`), empty SQL file (`rs_query.rs:305`) are all
  `ReadStatError::Other(String)` — callers will fragilely match strings. Add
  dedicated variants. Also reword library messages that reference CLI flags
  (`--compression-level`, `--overwrite` at `rs_write_config.rs:149, 211, 225, 233`)
  in terms of API arguments.
- **Misc**: `ParquetCompression` has `Display` but no `FromStr` (while `OutFormat`
  has both) and neither implements `PartialEq`/`Eq`/`Hash`.
  `ReadStatWriter::write_batch_to_parquet` / `merge_parquet_files`
  (`rs_write.rs:156, 201`) are CLI-orchestration internals in the public API and lack
  doc-cfg banners — consider `#[doc(hidden)]`. `ReadStatPath` accepts `.sas7bcat`
  (`rs_path.rs:13`) but catalogs aren't parseable — fails later with an opaque C
  error; reject up front.

---

## 4. Documentation accuracy

**Status:** ✅ done (2026-06-10). Landed:
- **CI memory-safety claims**: README.md table row and SECURITY.md now list the
  real checks (Miri, ASan Linux/macOS/Windows, weekly fuzzing; Valgrind manual) —
  no more phantom "Valgrind CI" / "unsafe audit".
- **MEMORY-SAFETY.md**: "four" → "five" jobs; added the experimental
  `asan-windows-full` (full C+Rust, `continue-on-error`) subsection; updated the
  `READSTAT_SANITIZE_ADDRESS` note, the coverage table's Windows row, and the
  Future-Work framing. Local Miri command now matches CI (`-- --skip property_tests`).
- **CHANGELOG.md**: backfilled the missing `[0.22.0]` (2026-05-12) and `[0.20.1]`
  (2026-03-03) entries from git history; sequence is now contiguous. (New-version
  stamp 0.24.0 / sys 0.5.0 deferred to release time — see checklist.)
- **mdBook links**: `build-book.sh` gained a portable `rewrite_links` (no GNU `\L`)
  applied in `patch_doc` and to the introduction, mapping `docs/FOO.md` / bare
  `FOO.md` / `../README.md` / `crates/*/` / `examples/*/` to the book's flat,
  lowercased filenames. `:balance_scale:` added to the emoji map. Verified bash
  syntax + sample rewrite.
- **`--sql` / `--sql-file` docs**: USAGE.md SQL section now states `--sql` is not a
  default feature and documents `cargo install readstat-cli --features sql`; added
  `--sql-file` documentation + examples and the mutual-exclusion rules.
- **crates/readstat/README.md** (crates.io landing): dropped the "Pure Rust …
  uses FFI to C" contradiction; added a `read_to_batch` / `read_metadata`
  quick-start; MSRV note; `../../docs/ARCHITECTURE.md` → absolute GitHub URL.
- **LGPL disclosure**: new License section in root README + a License section in
  `readstat-iconv-sys/README.md` stating Windows builds statically link
  LGPL-2.1+ libiconv (§6 relink obligation).
- **Counts/paths**: "29 test modules" → 30 (README, readstat-tests README,
  ARCHITECTURE.md); valgrind example test name `parse_file_metadata_test` →
  `parse_cars_md_test` (TESTING.md, MEMORY-SAFETY.md); USAGE.md `#cli-install` →
  `#package-cli-install`; BENCHMARKING flamegraph now `-p readstat-cli` from repo
  root + corrected criterion/flamegraph output paths; ARCHITECTURE.md no longer
  lists `pub(crate)` `ColumnBuilder` as a key public type; `progress.rs` `inc`
  doc comment corrected ("about to be processed", not "have been processed").
- **Refuted (no change)**: `cb.rs` has no `DECIMAL_PLACES` doc references; the
  README feature table already says "all enabled by default" with `sql` correctly
  marked opt-in; SECURITY.md "latest published version" is release-correct.
- LICENSE files (already tracked below): ✅.

Verified correct (no action): every CLI flag in README/USAGE/cheatsheet exists with
matching semantics; counts (118 formats, 14 datasets, 49 C files, 41 error codes);
crate versions in ARCHITECTURE.md; all crate-level doc examples; metadata field
lists and JSON shapes; install docs (`cargo install readstat-cli` → binary
`readstat`); libclang-not-needed messaging; CI-CD.md vs workflows; relative links on
GitHub; vendored ReadStat pin `3add3a5`.

Wrong:

- **README.md (~line 110) and SECURITY.md claim CI checks that don't exist**:
  "Valgrind … unsafe audit". Valgrind is manual-only (MEMORY-SAFETY.md itself says
  so); no unsafe-audit job/tool exists. Reword to what runs: Miri, ASan
  (Linux/macOS/Windows), weekly fuzzing; Valgrind manual.
- **MEMORY-SAFETY.md is stale the other way**: says "four CI checks" and "C not
  instrumented on Windows", but `asan-windows-full` (full C+Rust, fifth job,
  `continue-on-error`) exists in `main.yml:490`. Update count, coverage table, and
  Future Work framing.
- **CHANGELOG.md missing released versions 0.22.0 and 0.20.1** (git tags exist;
  entries jump 0.23.0 → 0.21.0 and 0.20.2 → 0.20.0). Also `[Unreleased]` holds
  breaking changes — stamp a new version (0.24.0 / sys 0.5.0) at release; don't ship
  HEAD as 0.23.0.
- **Published mdBook has broken links** (verified live on GitHub Pages):
  `build-book.sh` lowercases filenames but doesn't rewrite inter-doc links —
  usage.html links `BUILDING.html` (404; file is `building.html`),
  `../README.html#install` (404), plus `TESTING.md#fuzz-testing`, `TECHNICAL.md#…`,
  `../crates/...` patterns. Add sed rewrites in `patch_doc`.
- **`--sql` documented without the feature-install note**: `sql` is not a default
  feature, so the documented `cargo install readstat-cli` yields a binary where
  `--sql` is an unknown argument. Document
  `cargo install readstat-cli --features sql`. `--sql-file` is documented nowhere.
- **"29 test modules" is actually 30** (root README ~line 123,
  `crates/readstat-tests/README.md` line 5, ARCHITECTURE.md line 116). Consider
  dropping hard counts — they rot.
- **`crates/readstat/README.md`** (the crates.io landing page): no quick-start code
  example (`read_to_batch` is 3 lines and never shown); "Pure Rust library… uses FFI
  bindings to the ReadStat C library" reads as self-contradictory ("pure library"
  intended); relative `../../docs/ARCHITECTURE.md` link breaks on crates.io (use
  absolute GitHub URLs — readstat-cli's README is the model); no MSRV mention.
- **No LICENSE file ships in any of the four packages** (only the repo root; the
  vendored libraries' licenses ship, not the crate's). Copy the root LICENSE into
  each crate dir and add to the fixed include lists.
  **Status:** ✅ done (2026-06-10 — root LICENSE copied into all four crate dirs;
  verified present in all four `cargo package --list` outputs)
- **LGPL not stated user-facing**: SPDX (`LGPL-2.1-or-later AND MIT`) and deny.toml
  are correct, but neither the iconv-sys README nor the root README (badge says
  "License: MIT") mentions that **Windows builds statically link LGPL libiconv**,
  with §6 relink obligations for distributors of Windows binaries. Add a short note
  to both.
- **Smaller drift**: ARCHITECTURE.md lists `ColumnBuilder` under "Key public types"
  but it's `pub(crate)`; lib.rs:259 / readstat README feature-table header says "all
  enabled by default" while `sql` is opt-in (ARCHITECTURE.md phrases it right);
  TESTING.md:73 / MEMORY-SAFETY.md:123 valgrind example names a nonexistent test
  binary (`parse_file_metadata_test`); USAGE.md:7 anchor `#cli-install` should be
  `#package-cli-install`; BENCHMARKING.md has wrong-directory commands
  (`cargo flamegraph --bin readstat` from `crates/readstat` — the bin is in
  readstat-cli; criterion report path; stale flamegraph path); MEMORY-SAFETY.md's
  local Miri command omits `-- --skip property_tests`; `cb.rs:207,210,230` doc
  comments reference a removed `DECIMAL_PLACES` constant (broken intra-doc links);
  `progress.rs:30` says `inc` reports rows "processed" but it's called before
  parsing (`rs_data.rs:343`); SECURITY.md says "latest published version" though
  nothing is published yet.

---

## 5. Robustness / polish (medium)

CLI & library behavior:
- `TIMEw.1-3` (and `.7-9`) formats silently discard fractional seconds
  (`formats.rs:30-31, 146-149` — only `.4-6` map to microseconds), inconsistent with
  DATETIME's millisecond tier. Map `.1-3` to milliseconds (or fold into the µs path).
- `--stream-rows 0` builds a degenerate `[0,0,…,0,row_count]` offsets ladder
  (`common.rs:22-29` clamps the divisor but multiplies with the unclamped value) →
  ~1M empty chunk parses on a 1M-row file. Bind the clamped value once; add clap
  `value_parser!(u32).range(1..)` to `--stream-rows`.
- Parallel-write temp files use predictable names (`.readstat_temp_{i}.parquet`,
  `run.rs:556-573`) — two concurrent runs into one directory corrupt each other;
  temp files leak on error. Use `tempfile::Builder` (already a dep) + RAII cleanup.
- Progress bar: `parsing_started` (called per-chunk) replaces the `{pos}/{len}` bar
  with a spinner so the bar never shows; `inc()` runs *before* parsing so `--parallel`
  jumps to 100% immediately (`run.rs:59-74`, `rs_data.rs:342-346`). Call
  `parsing_started` once; `inc` after the chunk completes.
- `--compression-level` without `--compression` warns only via `log::warn`,
  invisible under env_logger's default `error` filter. Use clap
  `requires = "compression"` (same for the other `log::warn`s in
  `rs_write_config.rs:148-151, 217-222, 260-264` — the "will be overwritten!"
  warning is also invisible).
- `--parallel-write` help text wrong both ways (`cli.rs:98-100` vs `run.rs:370-371`):
  it only affects Parquet (silently ignored for Feather), and order *is* preserved.
  Fix text; warn when given with non-Parquet or without `--parallel`.
- Empty `--columns-file` (only comments/blanks) silently selects ALL columns
  (`run.rs:81-88`) — return an error instead.
- `metadata --no-progress` is accepted but dead (`cli.rs:30-32`, `run.rs:134`) —
  remove before it becomes a compatibility wart.
- Runtime errors exit with code 2 (`main.rs:12`), colliding with clap's usage-error
  code. Use 1 for runtime failures.
- `ChannelPartitionStream::execute` panics if DataFusion executes the partition
  twice — self-joins (`rs_query.rs:104-116`). Return a `DataFusionError` stream;
  document the single-execution limit on both public functions.
- `data --sql` without `--output` silently ignores the SQL (takes the metadata-only
  branch, `run.rs:338-349`); dead else-branch at `run.rs:503-512`. Also the CLI uses
  collect-all `execute_sql` rather than the streaming variants — fold onto streaming
  along with §2.4.
- Stdout CSV header isn't CSV-escaped (`rs_write.rs:582-590`) — variable names with
  commas/quotes (legal under `VALIDVARNAME=ANY`) yield a malformed header.
- `open_output` re-opens + drops the file on every batch after the first
  (`rs_write.rs:399, 436, 473, 511`) — open only when `!wrote_start`.
- Output extension check is case-sensitive (`rs_write_config.rs:174-196`) —
  `out.CSV` rejected; use `eq_ignore_ascii_case`.
- Metadata stdout prints the same value for "arrow logical" and "arrow physical"
  type (`rs_write.rs:662-666`); `run.rs:345-347` re-parses metadata already read at
  `run.rs:323-324`.

FFI / core (lower-severity from the unsafe review):
- `ReadStatParser::new()` doesn't check `readstat_parser_init` for NULL
  (`rs_parser.rs:29-34`) — next handler-set call would segfault in C. Check and
  propagate an error.
- `ReadStatBufferCtx::new(&[u8])` erases the buffer lifetime (`rs_buffer_io.rs:27-44`)
  — currently private-module-safe, but add a lifetime param + `PhantomData` so the
  compiler enforces the documented contract.
- `buffer_seek` uses unchecked `i64` addition with attacker-influenced offsets
  (`rs_buffer_io.rs:83-84`) — debug-build overflow panic inside `extern "C"`. Use
  `checked_add`.
- `checked_f64_to_i32` rejects exactly `i32::MAX` (strict `<`; `<=` would be correct
  for i32) — practically irrelevant, informational.
- SAS `TIME` values can be negative or > 86399 s (durations); stored into Arrow
  `Time32/Time64` outside the spec's valid range with no validation — doc note or
  range check.

Build system & scripts:
- No friendly error when submodules aren't initialized — `cc` dies with dozens of
  "No such file or directory". Add a sentinel-file `assert!` (file-existence check
  keeps the crates.io path working).
- `rerun-if-changed` doesn't watch the vendored C sources (emitting any directive
  disables default whole-package tracking) — editing vendor `.c`/`.h` or bumping the
  submodule doesn't rebuild. Add `cargo:rerun-if-changed=vendor/ReadStat/src` (and
  the libiconv equivalents). Also add `rerun-if-env-changed=READSTAT_SANITIZE_ADDRESS`
  (`readstat-sys/build.rs:83`).
- `buildtime_bindgen` writes into `src/` unconditionally
  (`readstat-sys/build.rs:225-233`, `readstat-iconv-sys/build.rs:93-98`) — in a
  registry checkout this poisons `.cargo-checksum.json`; the unsupported-target
  panic message even steers users into this path. Gate the src/ write (opt-in env
  var, or skip when `.cargo-checksum.json` exists).
- `release-check.sh:148` packaging check can never fail — greps for
  "warning: aborting", which modern cargo doesn't print, and the pipe masks the exit
  code. Test the exit code directly. (This is the check that would have caught §1.1.)
- `check-updates.sh:103-121,196` quarantine fails *open*: API errors yield age 999 →
  classified "safe" → `--apply` applies an unverifiable release. Fail closed; check
  `check-updates.ps1` for the same.
- `readstat-sys-ci.yml:14-16` header comment says the bindings diff is
  "informational" but the step `exit 1`s on drift. Also `git diff --exit-code`
  passes on *untracked* files — a brand-new bindings file silently passes; use
  `git status --porcelain`.
- `LIBCLANG_PATH` block (`readstat-sys/build.rs:106-131`) gates on
  *target* windows-msvc but asserts a *host* path — wrong under cross + bindgen.
- Bindings keyed by (os, arch) ignore `target_env`: `x86_64-pc-windows-gnu` would
  consume the MSVC file (enum signedness differs). Document windows-gnu as
  unsupported or key by env. No Windows-on-ARM bindings (clear panic message — fine,
  just a known gap).
- CI matrix gap: `ci-test` runs ubuntu-only; a PR touching only `crates/readstat/`
  gets zero macOS/Windows testing before tag time. Consider a small OS matrix.
- Cosmetic: duplicate link directives `cc` already emits; build.rs comment says
  "all four checked-in files" — there are five.

Release tooling:
- RELEASING.md: sys-crate publishes after `vendor.sh prepare` will trip cargo's VCS
  dirty check — document `--allow-dirty` for those two publishes (or commit vendored
  files on a throwaway release branch). Publish order (iconv-sys → sys → readstat →
  cli) is correctly documented and matches the dependency graph.
- `pre-release-replacements` duplicated between `crates/*/release.toml` and
  `[package.metadata.release]` (where the misplaced `include` from §1.1 lives) —
  consolidate after fixing §1.1.
- `tag-name = "v{{version}}"` shared between the two version lines — sys releases
  will eventually collide with existing tags. Consider `readstat-sys-v{{version}}`.

---

## 6. Plan of attack

1. **Blockers §1.1–1.5** — small, mechanical, all verified broken.
2. **Correctness §2.1–2.3** (corrupt-output-exit-0, datetime rounding, callback
   panics), then **§2.4–2.9**. Add tests with each fix: fractional-seconds
   round-trip, 0-row output, mid-file-error exit code, flush-on-finish.
3. **API/semver pass (§3)** — one focused pass over `pub` items, derives,
   re-exports. Involves judgment calls (variant renames, pub-field policy) — review
   together before locking in.
4. **Docs (§4)** — accuracy fixes, crates.io README quick-start, LICENSE files into
   each crate, LGPL note.
5. **Polish (§5)** as taste dictates; at minimum the build-script rerun/submodule
   items and the two failing-open scripts.

Pre-publish checklist once the above lands:
- `cargo hack check --feature-powerset -p readstat`
- `cargo package --list` for all four crates; confirm sizes and LICENSE presence
- `cargo msrv verify` (or CI job pinned to the declared MSRV)
- Fixed `vendor.sh prepare` → `cargo publish --dry-run` per crate in dependency order
- Re-run fuzz corpora against the fixed targets
- Stamp CHANGELOG (0.24.0 / sys 0.5.0), backfill 0.22.0 / 0.20.1 entries
