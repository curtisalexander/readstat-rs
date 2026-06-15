[< Back to README](../README.md)

# Improvement Roadmap

This document captures follow-up ideas for making `readstat-rs` easier to use, easier to understand, safer across the FFI boundary, and more polished for a crates.io release. It is intentionally detailed so future contributors can pick it up with fresh context.

## 1. Add a True Streaming `RecordBatch` API

### What “true streaming” means

Today the crate can process large files in chunks, but the public low-level workflow still asks callers to manually orchestrate chunking:

```rust,no_run
# fn main() -> Result<(), readstat::ReadStatError> {
let rsp = readstat::ReadStatPath::new("data.sas7bdat")?;
let mut md = readstat::ReadStatMetadata::new();
md.read_metadata(&rsp, false)?;

let offsets = readstat::build_offsets(md.row_count as u32, 10_000);
for pair in offsets.windows(2) {
    let mut d = readstat::ReadStatData::new().init(md.clone(), pair[0], pair[1]);
    d.read_data(&rsp)?;
    let batch = d.batch.take().unwrap();
    // caller writes / consumes batch
}
# Ok(())
# }
```

That is chunked reading, but it is not an ergonomic streaming API. A “true streaming” API would expose an iterator-like or stream-like abstraction that yields `RecordBatch` values one at a time, hiding offset math, metadata cloning, initialization order, and zero-row edge cases.

For a synchronous Rust API, “true streaming” can simply mean:

```rust,no_run
# fn main() -> Result<(), readstat::ReadStatError> {
let reader = readstat::RecordBatchReader::from_path("data.sas7bdat")?
    .with_batch_size(10_000)
    .with_columns(["name", "age"])?;

for batch in reader {
    let batch = batch?;
    println!("{} rows", batch.num_rows());
}
# Ok(())
# }
```

This is “streaming” because callers can consume batches incrementally without loading all rows into memory or manually managing chunks. It does **not** necessarily mean `async` streaming. An async adapter can come later if needed.

### Why this helps

- Makes the library API match the CLI’s chunked behavior.
- Helps users process large SAS files with bounded memory.
- Reduces error-prone manual use of `ReadStatData::init`, row offsets, and writer finalization.
- Gives downstream libraries a natural ingestion point for Arrow/DataFusion pipelines.
- Provides one place to encode edge cases: zero-row datasets, column projection, byte inputs, progress callbacks, and row-range clamping.

### Proposed API sketch

Names are placeholders; use whatever feels best after implementation spikes.

```rust,no_run
use readstat::arrow_array::RecordBatch;

pub struct RecordBatchReader {
    source: ReadSource,
    metadata: readstat::ReadStatMetadata,
    options: readstat::ReadOptions,
    batch_size: u32,
    next_row: u32,
    end_row: u32,
    column_mapping: Option<std::collections::BTreeMap<i32, i32>>,
}

pub enum ReadSource {
    Path(readstat::ReadStatPath),
    Bytes(std::sync::Arc<[u8]>),
    // Mmap could be an optimization later. Bytes already covers mmap contents if owned elsewhere.
}

impl RecordBatchReader {
    pub fn from_path<P: AsRef<std::path::Path>>(path: P) -> Result<Self, readstat::ReadStatError>;

    pub fn from_bytes(bytes: impl Into<std::sync::Arc<[u8]>>) -> Result<Self, readstat::ReadStatError>;

    pub fn with_batch_size(mut self, rows: u32) -> Self;

    pub fn with_options(mut self, options: readstat::ReadOptions) -> Result<Self, readstat::ReadStatError>;

    pub fn schema(&self) -> &readstat::arrow_schema::Schema;

    pub fn metadata(&self) -> &readstat::ReadStatMetadata;
}

impl Iterator for RecordBatchReader {
    type Item = Result<RecordBatch, readstat::ReadStatError>;

    fn next(&mut self) -> Option<Self::Item>;
}
```

### Implementation starting point

1. Add a new module, likely `crates/readstat/src/rs_reader.rs` or `batch_reader.rs`.
2. Reuse the existing high-level option code from `api.rs`:
   - `ReadOptions`
   - row range resolution
   - selected-column resolution
3. Make row-range resolution reusable internally. Today `resolve_row_range` is private in `api.rs`; either:
   - move it to a private common module, or
   - make a `pub(crate)` helper in `api.rs`, or
   - add methods on `ReadOptions`.
4. In `RecordBatchReader::from_path`:
   - create `ReadStatPath`
   - read metadata once
   - compute `end_row`
   - resolve column mapping once
   - store original metadata plus filtered metadata info as needed
5. In `Iterator::next`:
   - if `next_row >= end_row`, return `None`
   - compute `row_end = min(next_row + batch_size, end_row)` using checked/saturating arithmetic
   - initialize `ReadStatData` with either `init` or `init_filtered`
   - call `read_data` or `read_data_from_bytes`
   - take `d.batch`
   - advance `next_row`
   - return `Some(Ok(batch))`
6. Handle zero-row datasets:
   - returning no batches is probably most idiomatic for an iterator
   - alternatively provide `read_empty_batch()` or a flag if callers need a schema-only empty batch
7. Add integration tests:
   - batch sizes exactly divide row count
   - final short batch
   - `batch_size > row_count`
   - `row_count = 0` option
   - selected columns preserve dataset order
   - bytes source matches path source
   - errors are surfaced on malformed/truncated input

### Design questions

- Should `RecordBatchReader` yield zero batches for an empty dataset, or one empty `RecordBatch` with schema? Iterators usually yield zero items, but writers often need a schema to create valid empty output. A separate `schema()` accessor may be enough.
- Should this implement Arrow’s `RecordBatchReader` trait? Arrow has `arrow_array::RecordBatchReader` in some versions. If practical, implementing it improves interoperability, but a crate-owned iterator is easier to start with.
- Should byte input borrow `&[u8]` or own `Arc<[u8]>`? Owning `Arc<[u8]>` avoids lifetime-heavy public types.
- Should there be a parallel variant? Start synchronous/sequential first; parallel batch production can be a later builder option.

## 2. Consider a `ReaderBuilder` API

A builder can make path/bytes/mmap, projection, row ranges, chunk size, and output mode discoverable in one place.

### Why this helps

The current API has several entry points:

- `read_metadata`
- `read_to_batch`
- `read_to_batch_with_options`
- `ReadStatPath`
- `ReadStatMetadata`
- `ReadStatData`
- writer helpers

They are useful, but new users may not know when to move from convenience functions to low-level orchestration. A builder can become the primary onboarding path.

### Possible API sketch

```rust,no_run
# fn main() -> Result<(), readstat::ReadStatError> {
let reader = readstat::ReaderBuilder::new()
    .batch_size(10_000)
    .columns(["name", "age"])
    .row_start(100)
    .row_count(1_000)
    .from_path("data.sas7bdat")?;

for batch in reader.batches()? {
    let batch = batch?;
    // consume batch
}

let one_batch = readstat::ReaderBuilder::new()
    .columns(["name", "age"])
    .from_path("data.sas7bdat")?
    .read_all()?;
# Ok(())
# }
```

### Implementation starting point

- Implement `ReaderBuilder` as a thin wrapper around `ReadOptions` plus `batch_size`.
- Keep `read_to_batch*` convenience functions and have them call the builder internally.
- Avoid replacing low-level types immediately; the builder should be additive and semver-safe.
- Add rustdoc examples showing “simple”, “project columns”, “stream batches”, and “read bytes”.

## 3. Reduce Public Mutable Fields Over Time

### Current state

Several core structs expose internal state publicly for historical convenience, especially:

- `ReadStatMetadata`
- `ReadStatData`
- `ReadStatVarMetadata`

This makes experimentation easy, but it also makes invariants harder to protect. For example, `ReadStatData` has fields that must remain consistent with one another:

- `var_count`
- `vars`
- `builders`
- `schema`
- `chunk_rows_to_process`
- `chunk_row_start`
- `chunk_row_end`
- `column_filter`
- `total_var_count`

If a caller mutates these directly, they can create impossible states that only fail later during FFI callbacks or Arrow batch construction.

### Why this helps

- Safer public API.
- Better semver flexibility: private fields can change without breaking users.
- Clearer invariants around FFI callback contexts.
- Less burden on users to understand initialization ordering.

### Suggested approach

Do this gradually; avoid a large breaking change before the API shape settles.

1. Add accessor methods now:
   - `ReadStatMetadata::row_count()`
   - `ReadStatMetadata::var_count()`
   - `ReadStatMetadata::schema()`
   - `ReadStatMetadata::variables()`
   - `ReadStatData::batch()` / `into_batch()`
2. Update internal examples and docs to prefer accessors.
3. Mark direct field usage as “low-level/unstable invariant-sensitive” in docs.
4. In a future semver-major release, make invariant-sensitive fields private.
5. Keep metadata fields that are naturally data-only public if desired, but hide fields that coordinate parser state.

### Implementation sketch

```rust,no_run
impl ReadStatData {
    pub fn batch(&self) -> Option<&readstat::arrow_array::RecordBatch> {
        self.batch.as_ref()
    }

    pub fn into_batch(self) -> Option<readstat::arrow_array::RecordBatch> {
        self.batch
    }
}
```

Then update examples from:

```rust,ignore
if let Some(batch) = &d.batch { ... }
```

to:

```rust,ignore
if let Some(batch) = d.batch() { ... }
```

## 4. Release Polish and Crates.io Readiness

### Current strengths

The repo already has a strong release foundation:

- `docs/RELEASING.md`
- release scripts
- `cargo package` dry-runs in release checks
- `cargo deny`
- docs.rs metadata
- vendoring scripts for git submodules
- clear MSRV and platform notes
- Arrow/DataFusion lockstep checks

### Improvements to consider

#### Add a release readiness checklist doc or issue template

Create something like `docs/RELEASE-READINESS.md` or a GitHub issue template with a concrete checklist:

- [ ] `CHANGELOG.md` entry exists
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace --all-features`
- [ ] `cargo doc --workspace --all-features --no-deps`
- [ ] `cargo deny check`
- [ ] `scripts/check-arrow-lockstep.sh`
- [ ] `scripts/check-updates.sh`
- [ ] `scripts/check-vendor-updates.sh`
- [ ] `cargo package --list -p <crate>` reviewed for all publishable crates
- [ ] docs.rs build locally approximated
- [ ] README examples compile as doctests where possible
- [ ] license notes reviewed, especially Windows LGPL/static libiconv language

#### Consider CI packaging checks on every PR

If not already present in CI, add a job that runs:

```sh
cargo package -p readstat --allow-dirty
cargo package -p readstat-cli --allow-dirty
cargo package -p readstat-sys --allow-dirty
cargo package -p readstat-iconv-sys --allow-dirty
```

For the sys crates, this may require either initialized submodules or the vendor prepare flow. The job should catch missing files, bad include/exclude patterns, broken crate README links, and publish-time surprises.

#### Verify crate-level READMEs are first-class

Crates.io shows each crate’s own README, not the workspace README. Keep these polished:

- `crates/readstat/README.md`
- `crates/readstat-cli/README.md`
- `crates/readstat-sys/README.md`
- `crates/readstat-iconv-sys/README.md`

Each should answer:

- What is this crate for?
- Should most users depend on this crate directly?
- What platforms are supported?
- What features are available?
- What license obligations apply?

## 5. More Onboarding and Discoverability

### Add an “Which API should I use?” section

Good candidates:

- root `README.md`
- `crates/readstat/README.md`
- crate-level rustdocs
- mdBook intro

Possible table:

| Goal | Use |
|------|-----|
| Read metadata | `read_metadata` |
| Read whole file into Arrow | `read_to_batch` |
| Read selected columns / preview rows | `read_to_batch_with_options` + `ReadOptions` |
| Process large files incrementally | future `RecordBatchReader` |
| Write CSV/Parquet/Feather/NDJSON | `ReadStatWriter` or CLI |
| Expose upload/convert over HTTP | `examples/api-demo` |
| Use in browser/JS | `readstat-wasm` |
| Work with raw C ReadStat API | `readstat-sys` |

### Expand visual docs over time

`docs/VISUAL-GUIDE.md` is a text-based starting point. Future improvements:

- Add Mermaid diagrams if the mdBook/GitHub Pages pipeline supports them.
- Add a “callback safety checklist”.
- Add a “new output writer checklist”.
- Add a “new input format checklist” if SPSS/Stata support becomes a goal.

## 6. Memory Safety and FFI Boundary Hardening

### Good current practices

The code already has several safety-minded choices:

- RAII wrapper for `readstat_parser_t`.
- Null parser allocation check.
- Callback-specific abort errors instead of panicking across `extern "C"`.
- Checked date/time conversions.
- Preallocation caps for untrusted row counts and storage widths.
- Miri, ASan, fuzzing, and Valgrind documentation.

### Future hardening ideas

- Add explicit `debug_assert!` or error checks in callbacks for impossible indices before indexing builders.
- Continue moving unchecked casts toward `try_from` at API boundaries.
- Audit all `unsafe` blocks and add short local `// SAFETY:` comments where missing.
- Consider a private wrapper for callback context pointers to document lifetime invariants.
- Add fuzz targets specifically for byte-slice metadata and projected data reads if not already present.

### Callback safety checklist sketch

For every callback:

- Never panic.
- Never unwind across C.
- Treat all file-derived counts, indices, widths, and timestamps as untrusted.
- Null-check C pointers unless upstream guarantees non-null and the guarantee is documented.
- Convert lossy/non-UTF-8 strings intentionally.
- Store typed errors in context and return `READSTAT_HANDLER_ABORT` when a recoverable Rust error occurs.
- Prefer checked arithmetic and checked integer conversions.

## 7. Potential API Direction

A layered API may be the most understandable long-term shape:

1. **One-call convenience**
   - `read_metadata`
   - `read_to_batch`
   - `read_to_batch_with_options`
2. **Builder / reader layer**
   - `ReaderBuilder`
   - `RecordBatchReader`
3. **Low-level expert layer**
   - `ReadStatPath`
   - `ReadStatMetadata`
   - `ReadStatData`
   - `ReadStatWriter`
4. **Raw FFI layer**
   - `readstat-sys`

The key is to make the common path obvious while preserving the power needed by the CLI and advanced integrations.

## Suggested First PR When Returning

A small, high-value next PR would be:

1. Add `RecordBatchReader` for path input only.
2. Support `ReadOptions` and configurable `batch_size`.
3. Implement `Iterator<Item = Result<RecordBatch, ReadStatError>>`.
4. Add tests for batch sizing, projection, and final short batch.
5. Add docs showing how it replaces manual `build_offsets` usage.

This keeps scope manageable while delivering the biggest API/discoverability win.
