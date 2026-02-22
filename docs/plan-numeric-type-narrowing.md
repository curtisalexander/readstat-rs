# Plan: Numeric Type Narrowing for SAS Columns

## Problem Statement

All numeric values in SAS `.sas7bdat` files are stored as 64-bit IEEE 754
doubles. The ReadStat C library reports them as `READSTAT_TYPE_DOUBLE`, and
`readstat-rs` maps every unformatted numeric column to Arrow `Float64`.

In practice many SAS columns contain only values that could be represented with
a smaller or more precise type:

| Column contents           | Wasteful type | Ideal type         | Savings per value |
|---------------------------|---------------|--------------------|-------------------|
| 0 / 1 flags               | Float64 (8 B) | Boolean (1 bit)    | ~8 B              |
| Small integers (0–255)    | Float64 (8 B) | Int8 or UInt8 (1 B)| 7 B               |
| Integers up to ±32 767    | Float64 (8 B) | Int16 (2 B)        | 6 B               |
| Integers up to ±2 billion | Float64 (8 B) | Int32 (4 B)        | 4 B               |
| Large integers (no frac.) | Float64 (8 B) | Int64 (8 B)        | 0 B (but semantically cleaner) |

The question: should `readstat-rs` attempt to detect these cases and produce
narrower Arrow types?

---

## Current Architecture (Relevant Parts)

### Type Decision Point

Type is decided in `rs_metadata.rs:85–118` (`initialize_schema`), *before any
row data is read*. For `ReadStatVarType::Double` without a temporal format
class, the result is unconditionally `DataType::Float64`.

### Value Ingestion

`cb.rs:349–416` (`handle_value`, `READSTAT_TYPE_DOUBLE` arm) extracts the raw
double via `readstat_double_value()`, rounds it, and appends it to the
pre-allocated `Float64Builder`. The builder type is locked at schema-creation
time; there is no mechanism to change it mid-stream.

### Streaming Pipeline

The CLI streams data in chunks (default 10,000 rows). Each chunk creates a
fresh `ReadStatData`, allocates builders matching the schema, reads rows via
FFI callbacks, then finishes builders into a `RecordBatch`. Chunks are
pipelined to a writer thread via a bounded crossbeam channel.

**Key constraint:** The Arrow schema is fixed before the first row is read.
Builders are typed at allocation time. There is no facility to "rewind" and
re-type a column after observing its values.

---

## Approaches

### Approach A: Two-Pass File Scan

Read the file twice:

1. **Pass 1 (scan):** Read all (or N) rows, accumulate per-column statistics
   (`has_fractional`, `min`, `max`, `all_null`, `has_null`). Do *not* build
   Arrow arrays.
2. **Pass 2 (ingest):** Build schema using scan results, then do the normal
   read with narrowed builders.

**Sketch of scan-phase metadata:**

```rust
struct ColumnStats {
    has_fractional: bool,   // any value where v != v.trunc()
    min: f64,
    max: f64,
    all_null: bool,
    has_null: bool,
    count: u64,
}
```

**Type narrowing rules (after scan):**

```
if all_null               → keep Float64 (or Null type)
if has_fractional         → Float64 (or Float32 if range fits)
if !has_fractional:
    if min >= 0 && max <= 1         → Boolean  (optional)
    if min >= i8::MIN  && max <= i8::MAX   → Int8
    if min >= i16::MIN && max <= i16::MAX  → Int16
    if min >= i32::MIN && max <= i32::MAX  → Int32
    if min >= i64::MIN && max <= i64::MAX  → Int64
    else                                   → Float64 (precision loss)
```

**Pros:**

- Produces the tightest possible types.
- Works with both streaming and mem readers (scan is always a full pass).
- Schema is finalized before ingestion — no mid-stream builder swaps.

**Cons:**

- **Doubles I/O and CPU for the scan pass.** For a 10 GB file over NFS this
  is significant. ReadStat must decompress and decode every page twice.
- Scan pass still requires FFI callbacks and pointer casts — not free.
- Adds an entirely new code path that must be maintained.
- `--rows N` (row-limit) interacts awkwardly: scan N rows, then read N rows.

### Approach B: Scan-Then-Cast (Single Pass with Post-Processing)

Read data normally into `Float64` builders (no schema change), then after
each chunk (or after reading the entire file), analyze the finished
`Float64Array` and `cast()` it to a narrower type.

**Sketch:**

```rust
// After finishing builders into a RecordBatch:
fn narrow_batch(batch: &RecordBatch) -> RecordBatch {
    let narrowed: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(col, field)| {
            if col.data_type() == &DataType::Float64 {
                try_narrow_float64(col)
            } else {
                Arc::clone(col)
            }
        })
        .collect();
    RecordBatch::try_new(narrowed_schema, narrowed).unwrap()
}
```

Arrow's `cast` kernel handles `Float64 → Int32`, etc.

**Pros:**

- Single read pass — no I/O penalty.
- Leverages Arrow's existing, optimized `cast` kernels.
- Can be done per-chunk, fitting naturally into the streaming pipeline.
- Simpler implementation — the FFI layer is untouched.

**Cons:**

- **Per-chunk type decisions can disagree.** Chunk 1 might see only integers
  and narrow to `Int32`, while chunk 2 sees a fractional value and stays
  `Float64`. The writer then receives batches with inconsistent schemas.
  Parquet and Feather both require a single schema.
- Solving the above requires either:
  - (a) A pre-scan pass (back to Approach A's cost), or
  - (b) Buffering all chunks in memory before deciding (defeats streaming), or
  - (c) Choosing a "safe" type per-chunk and widening at the writer if a
    later chunk disagrees — this adds significant complexity.
- Temporary memory spike: the full `Float64Array` exists alongside the
  narrowed copy until the original is dropped.
- Post-hoc narrowing of streaming chunks produces a moving-target schema that
  complicates downstream consumers.

### Approach C: Sample-Based Heuristic (Partial Scan)

Scan only the first N rows (user-configurable, e.g. `--scan-rows 1000`), then
lock the schema and stream normally.

**Pros:**

- Overhead proportional to N, not the file size.
- Familiar pattern from CSV readers (pandas `low_memory`, polars
  `infer_schema_length`).
- Schema locked before full ingest — streaming works.

**Cons:**

- **Wrong guesses.** Row 1,001 could contain a fractional value after the
  scanner committed to `Int32`. This must either:
  - Abort with an error (bad UX).
  - Silently truncate (data corruption).
  - Fall back to a wider type mid-stream (requires swapping builders,
    rebuilding already-emitted chunks — essentially impossible in a
    streaming pipeline without buffering everything).
- The "how many rows to scan" question is inherently unanswerable. Any
  default is wrong for some dataset.
- CSV readers get away with this because they have *no* type information
  at all. SAS files *do* declare the type (double) — overriding it with a
  heuristic is a step backward in reliability.

### Approach D: User-Declared Type Overrides

Let the user specify desired types per column via CLI flags or a schema file:

```bash
readstat data input.sas7bdat --output out.parquet \
    --type-override "flag_col=bool,count_col=int32"
```

**Pros:**

- No scanning, no heuristics, no wrong guesses.
- User is in full control and accepts responsibility for correctness.
- Minimal library complexity — just a cast step after each batch.
- Works perfectly with streaming.

**Cons:**

- Requires the user to know their data.
- Per-column specification is tedious for wide datasets (hundreds of cols).
- Still need runtime validation: if a value doesn't fit the declared type,
  what happens? Truncate? Error? Null?

### Approach E: Post-Conversion Optimization (Outside readstat-rs)

Don't change `readstat-rs` at all. Users who need narrower types do it
downstream:

- In **Polars/DuckDB/DataFusion**: `SELECT CAST(col AS INT) FROM read_parquet('file.parquet')`
- In **Arrow/pyarrow**: `table = pq.read_table('file.parquet')` then
  `table.cast(narrowed_schema)`
- In **Parquet itself**: Parquet uses dictionary and RLE encoding. A column
  of float64 values `[0.0, 1.0, 0.0, 1.0, ...]` compresses extremely well
  — likely to 1–2 bits per value regardless of the declared Float64 type.

**Pros:**

- Zero changes to `readstat-rs`.
- Users who care about types can handle it with mature, well-tested tools.
- Parquet's encoding already solves the storage-size concern for on-disk
  formats.
- No risk of introducing bugs or wrong-type guesses in the converter.

**Cons:**

- Doesn't help in-memory size for CSV/NDJSON output consumers.
- Adds a post-processing step to the user's workflow.
- Doesn't address the "it would be nice if the Parquet schema was Int32"
  aesthetic preference.

---

## Key Technical Challenges

### 1. Schema Must Be Known Before Data

Arrow's `RecordBatch` requires all columns in a batch to conform to a single
`Schema`. The current design allocates typed builders from the schema before
any rows are read. Changing types mid-stream means either:

- Re-allocating builders and copying already-appended values (expensive, complex).
- Buffering all data and deciding at the end (defeats streaming, spikes memory).
- Two-pass reading (doubles I/O).

### 2. Streaming Chunks Must Share a Schema

The writer (Parquet, Feather) expects every `RecordBatch` to match the same
schema. If chunk 1 narrows a column to `Int32` but chunk 5 needs `Float64`,
all previous chunks must be rewritten — impossible in a streaming pipeline.

### 3. SAS Numeric Precision Edge Cases

SAS stores numbers as 8-byte doubles but allows `LENGTH` statements to
truncate storage to as few as 3 bytes. A column declared `LENGTH x 3;` can
only store integers up to ~8,192. The ReadStat C library restores these to
full doubles. We could use SAS `LENGTH` as a hint, but it's rarely set
explicitly and doesn't guarantee integer content.

SAS format strings (e.g. `BEST12.`, `COMMA9.`) describe *display* format,
not storage constraints. `BEST12.` is the default for all numerics and conveys
no type information.

### 4. Missing Values

SAS has 28 distinct missing values (`.`, `.A`–`.Z`, `._`). ReadStat reports
them all as `readstat_value_is_system_missing() == 1`. In Arrow, nulls are
represented by a validity bitmap, orthogonal to the data type. A column with
missing values can still be narrowed — but the scanner must handle them
(skip, don't treat as 0.0 or NaN).

### 5. Boolean Representation

SAS has no boolean type. Conventions vary: some use 0/1, some use 0/1 with
missing, some use 1/2, some use character 'Y'/'N'. There's no reliable way
to auto-detect "this is a boolean column" without user guidance.

---

## Comparison with CSV Readers

| Aspect | CSV readers | readstat-rs |
|--------|-------------|-------------|
| Source type info | None (all text) | Declared (Float64) |
| Type inference | Required for usability | Optional optimization |
| Wrong guess consequence | Common, expected, documented | Surprising — "why did my float get truncated?" |
| User expectation | "Types may be wrong" | "Types should match the source" |
| Established practice | pandas/polars/DuckDB all scan | No SAS reader does this |

CSV readers *must* infer types because the format carries none. SAS files
*declare* types. Overriding the declared type is a fundamentally different
contract with the user.

---

## Memory / Performance Analysis

**Memory savings (hypothetical 1M-row, 50-column dataset, all numeric):**

| Scenario | Float64 | Int32 (if applicable) | Savings |
|----------|---------|----------------------|---------|
| In-memory | 50 × 1M × 8 B = 400 MB | 50 × 1M × 4 B = 200 MB | 200 MB (50%) |
| Parquet on disk | ~50–100 MB (compressed) | ~25–50 MB (compressed) | Marginal — Parquet encoding already compresses well |
| CSV on disk | N/A (text) | N/A (text) | Zero — CSV doesn't store types |

The savings are real for in-memory consumers (Arrow IPC, Feather) but modest
for Parquet (which already handles this at the encoding layer). For CSV and
NDJSON output the on-disk size is identical.

**Performance cost of two-pass scanning:**

- Approximately 2× the read time for the scan pass. For a 10 GB file this
  could add 30–60 seconds depending on I/O.
- The scan pass is simpler than full ingestion (no builder allocation, just
  statistics), so it won't be a full 2×, but it still requires ReadStat to
  decompress and decode every page.

---

## Recommendation

Given the trade-offs, I'd rank the approaches:

1. **Approach E (do nothing)** — strongest option for now. Parquet compression
   already handles the storage concern. Users who need specific types can cast
   downstream with one line of SQL or Python. Zero risk, zero complexity.

2. **Approach D (user-declared overrides)** — good if there's real user
   demand. Modest implementation effort, no heuristics, works with streaming.
   Could be added incrementally.

3. **Approach A (two-pass scan)** — technically sound, opt-in behind a flag
   like `--narrow-types`. Significant implementation effort and a permanent
   maintenance burden. Only worthwhile if users are regularly hitting memory
   limits that narrowing would alleviate.

4. **Approach B (post-cast per chunk)** — the schema-inconsistency problem
   between chunks makes this fragile. Not recommended unless combined with
   Approach A's pre-scan.

5. **Approach C (sample-based heuristic)** — the wrong-guess failure mode is
   too dangerous for a tool that converts production data. CSV readers accept
   this risk because they have no choice; we do.

**My honest assessment:** this feature would add meaningful complexity for a
narrow benefit. The main beneficiaries are users who (a) load the output into
Arrow/Feather (not Parquet, which compresses well regardless), (b) have large
datasets with many integer-only columns, and (c) are memory-constrained. For
most users, the Float64 output is correct, expected, and compatible with
everything.

If we were to proceed, Approach D (user overrides) offers the best
complexity-to-value ratio as a first step, with Approach A (opt-in two-pass
scan) as a possible future addition if demand materializes.

---

## If We Proceed: Implementation Sketch for Approach D

### CLI Changes (`readstat-cli/src/cli.rs`)

```
--type-override <SPEC>    Comma-separated column=type pairs
                          Types: bool, int8, int16, int32, int64, float32, float64
                          Example: --type-override "flag=bool,count=int32"
```

### Library Changes (`readstat/src/`)

1. **New type:** `TypeOverrides` — a `HashMap<String, DataType>` parsed from
   the CLI spec.

2. **Schema modification** (`rs_metadata.rs`): After `initialize_schema()`,
   apply overrides — replace field types for matching column names.

3. **Builder selection** (`rs_data.rs`): `ColumnBuilder::from_metadata` would
   check for overrides and allocate the narrower builder.

4. **Value callback** (`cb.rs`): For overridden `DOUBLE` columns, cast the
   `f64` to the target type at append time. E.g. for an `Int32` override:
   ```rust
   let v = round_decimal_f64(raw);
   if v != v.trunc() || v < i32::MIN as f64 || v > i32::MAX as f64 {
       // value doesn't fit — error or null
   }
   builder.append_value(v as i32);
   ```

5. **Error handling:** Values that don't fit the declared type → configurable
   behavior (error, null, or truncate). Default should be error.

### Files Modified

| File | Change |
|------|--------|
| `crates/readstat-cli/src/cli.rs` | Add `--type-override` arg |
| `crates/readstat/src/rs_metadata.rs` | Apply overrides to schema |
| `crates/readstat/src/rs_data.rs` | New `ColumnBuilder` variants or override-aware allocation |
| `crates/readstat/src/cb.rs` | Cast-at-append logic for overridden columns |
| `crates/readstat/src/rs_var.rs` | Possibly new override types |
| `crates/readstat-tests/` | New test module for type overrides |

### Estimated Scope

Moderate — touches the FFI callback hot path, schema initialization, and CLI
parsing. Needs thorough testing for edge cases (overflow, NaN, missing values
in narrowed columns). Does *not* require changes to the ReadStat C library or
`readstat-sys`.
