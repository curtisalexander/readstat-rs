# Plan: Replace Vec<ReadStatVar> with Arrow Builders (StringBuilder)

## Problem

The parse phase dominates runtime (~91%, 35s on AHS 2019 dataset). The bottleneck is
`ptr_to_string()` in `handle_value`, which allocates a new `String` per cell via
`String::from_utf8_lossy(cstr.to_bytes()).to_string()`. For the AHS dataset (63,185 rows
x 2,512 string columns = ~159M string cells), this means 159M individual heap allocations.

These Strings are stored in `Vec<Vec<ReadStatVar>>`, then during `cols_to_batch` they are
borrowed as `&str` and fed into `StringArray::from_iter`. The intermediate `ReadStatVar`
enum + per-cell `String` allocation is pure waste — we can append `&str` directly into
Arrow's contiguous buffer during the FFI callback.

## Current Architecture

```
handle_value (per cell, ~159M calls for AHS)
  → ReadStatVar::get_readstat_value()
    → ptr_to_string(readstat_string_value(value))  // allocates String
    → returns ReadStatVar::ReadStat_String(Some(string))
  → d.cols[col_index].push(value)                  // stores in Vec<ReadStatVar>

cols_to_batch (once per chunk)
  → for each col: StringArray::from_iter(col.iter().map(|s| s.as_deref()))
  → RecordBatch::try_new(schema, arrays)
```

**Key files:**
- `crates/readstat/src/rs_data.rs` — `ReadStatData` struct, `cols_to_batch`, `allocate_cols`
- `crates/readstat/src/cb.rs` — `handle_value` callback (line 188)
- `crates/readstat/src/rs_var.rs` — `ReadStatVar` enum, `get_readstat_value`
- `crates/readstat/src/common.rs` — `ptr_to_string`

## Proposed Architecture

Replace `cols: Vec<Vec<ReadStatVar>>` with typed Arrow builders that accumulate values
directly during `handle_value`. For strings, use `StringBuilder` which maintains a single
contiguous byte buffer — each `append_value(&str)` is a memcpy into that buffer, no
per-cell heap allocation.

```
handle_value (per cell)
  → match value_type {
      STRING => {
        let cstr = CStr::from_ptr(readstat_string_value(value));
        let s = cstr.to_str().unwrap_or(/* lossy fallback */);
        builders[col_index].as_string_mut().append_value(s);  // memcpy, no alloc
      }
      DOUBLE => {
        builders[col_index].as_f64_mut().append_value(parsed_value);
      }
      ...
    }

cols_to_batch (once per chunk)
  → for each builder: builder.finish() → ArrayRef    // just pointer arithmetic
  → RecordBatch::try_new(schema, arrays)
```

## Detailed Changes

### 1. New enum: `ColumnBuilder` (new file or in `rs_data.rs`)

A wrapper enum over Arrow's typed builders:

```rust
use arrow_array::builder::*;

enum ColumnBuilder {
    Str(StringBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Date32(Date32Builder),
    TimestampSecond(TimestampSecondBuilder),
    TimestampMillisecond(TimestampMillisecondBuilder),
    TimestampMicrosecond(TimestampMicrosecondBuilder),
    TimestampNanosecond(TimestampNanosecondBuilder),
    Time32Second(Time32SecondBuilder),
    Time64Microsecond(Time64MicrosecondBuilder),
}

impl ColumnBuilder {
    fn finish(&mut self) -> ArrayRef { ... }
}
```

### 2. Modify `ReadStatData` struct (`rs_data.rs`)

Replace `cols: Vec<Vec<ReadStatVar>>` with `builders: Vec<ColumnBuilder>`:

```rust
pub struct ReadStatData {
    // REMOVE: pub cols: Vec<Vec<ReadStatVar>>,
    // ADD:
    pub builders: Vec<ColumnBuilder>,
    // ... rest unchanged
}
```

### 3. Modify `allocate_cols` → `allocate_builders` (`rs_data.rs`)

Create typed builders based on variable metadata. Use `storage_width` for string capacity hints:

```rust
pub fn allocate_builders(self) -> Self {
    let chunk_rows = self.chunk_rows_to_process;
    let builders: Vec<ColumnBuilder> = self.vars.values().map(|vm| {
        match (&vm.var_type_class, &vm.var_format_class) {
            (ReadStatVarTypeClass::String, _) => {
                // Pre-size: chunk_rows entries, chunk_rows * storage_width bytes
                ColumnBuilder::Str(StringBuilder::with_capacity(
                    chunk_rows,
                    chunk_rows * vm.storage_width,
                ))
            }
            (ReadStatVarTypeClass::Numeric, None) => {
                // Check var_type for int8/int16/int32/float/double
                match vm.var_type {
                    ReadStatVarType::Int8 => ColumnBuilder::Int8(Int8Builder::with_capacity(chunk_rows)),
                    ReadStatVarType::Int16 => ColumnBuilder::Int16(Int16Builder::with_capacity(chunk_rows)),
                    ReadStatVarType::Int32 => ColumnBuilder::Int32(Int32Builder::with_capacity(chunk_rows)),
                    ReadStatVarType::Float => ColumnBuilder::Float32(Float32Builder::with_capacity(chunk_rows)),
                    _ => ColumnBuilder::Float64(Float64Builder::with_capacity(chunk_rows)),
                }
            }
            (ReadStatVarTypeClass::Numeric, Some(fc)) => {
                match fc {
                    ReadStatVarFormatClass::Date => ColumnBuilder::Date32(Date32Builder::with_capacity(chunk_rows)),
                    ReadStatVarFormatClass::DateTime => ColumnBuilder::TimestampSecond(...),
                    // ... etc for each format class
                }
            }
        }
    }).collect();
    Self { builders, ..self }
}
```

### 4. Modify `handle_value` callback (`cb.rs`)

This is the critical change. Instead of creating a `ReadStatVar` and pushing to `Vec`,
append directly to the builder. For strings, avoid `ptr_to_string` entirely:

```rust
pub extern "C" fn handle_value(...) -> c_int {
    let d = unsafe { &mut *(ctx as *mut ReadStatData) };
    let var_index = unsafe { readstat_sys::readstat_variable_get_index(variable) };
    let value_type = unsafe { readstat_sys::readstat_value_type(value) };
    let is_missing = unsafe { readstat_sys::readstat_value_is_system_missing(value) };

    // ... column filter logic unchanged ...

    let builder = &mut d.builders[col_index as usize];

    match value_type {
        readstat_sys::readstat_type_e_READSTAT_TYPE_STRING
        | readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF => {
            let sb = builder.as_string_mut(); // helper method on ColumnBuilder
            if is_missing == 1 {
                sb.append_null();
            } else {
                let ptr = unsafe { readstat_sys::readstat_string_value(value) };
                if ptr.is_null() {
                    sb.append_null();
                } else {
                    let cstr = unsafe { CStr::from_ptr(ptr) };
                    // Fast path: try valid UTF-8 first (vast majority of SAS data)
                    match cstr.to_str() {
                        Ok(s) => sb.append_value(s),
                        Err(_) => {
                            // Lossy fallback for rare non-UTF-8 data
                            let s = String::from_utf8_lossy(cstr.to_bytes());
                            sb.append_value(s.as_ref());
                        }
                    }
                }
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE => {
            // Date/time dispatch still needed — use var_format_class from metadata
            let vm = &d.vars[&col_index];
            if is_missing == 1 {
                builder.append_null(); // generic null append
            } else {
                let raw = unsafe { readstat_sys::readstat_double_value(value) };
                let formatted = format!("{1:.0$}", DIGITS, raw);
                let val: f64 = lexical::parse(&formatted).unwrap();
                match vm.var_format_class {
                    None => builder.as_f64_mut().append_value(val),
                    Some(ReadStatVarFormatClass::Date) => {
                        builder.as_date32_mut().append_value((val as i32) - DAY_SHIFT);
                    }
                    // ... etc for each format class
                }
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 => { ... }
        // ... other numeric types
    }

    // row boundary tracking unchanged
    if var_index == (d.total_var_count - 1) { ... }

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}
```

### 5. Simplify `cols_to_batch` (`rs_data.rs`)

The conversion phase becomes trivial — just `finish()` each builder:

```rust
pub fn cols_to_batch(&mut self) -> Result<(), ReadStatError> {
    let arrays: Vec<ArrayRef> = self.builders
        .iter_mut()
        .map(|b| b.finish())
        .collect();

    self.batch = Some(RecordBatch::try_new(Arc::new(self.schema.clone()), arrays)?);
    Ok(())
}
```

### 6. Remove `ReadStatVar` enum usage from hot path

- `ReadStatVar::get_readstat_value()` is no longer called from `handle_value`
- The enum and its method can remain for other uses (tests, direct API consumers)
- `ptr_to_string()` is no longer called in the string hot path

### 7. Helper methods on `ColumnBuilder`

```rust
impl ColumnBuilder {
    fn as_string_mut(&mut self) -> &mut StringBuilder {
        match self { ColumnBuilder::Str(b) => b, _ => panic!("not a string builder") }
    }
    fn as_f64_mut(&mut self) -> &mut Float64Builder { ... }
    // ... etc for each type

    fn append_null(&mut self) {
        match self {
            ColumnBuilder::Str(b) => b.append_null(),
            ColumnBuilder::Int8(b) => b.append_null(),
            // ... etc
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Str(b) => Arc::new(b.finish()),
            ColumnBuilder::Int8(b) => Arc::new(b.finish()),
            // ... etc
        }
    }
}
```

## Migration Strategy

### Phase 1: Add ColumnBuilder alongside existing code
- Create `ColumnBuilder` enum and helper methods
- Add `builders: Vec<ColumnBuilder>` field to `ReadStatData`
- Implement `allocate_builders()` based on variable metadata
- Keep `cols` and existing `cols_to_batch` working

### Phase 2: New handle_value using builders
- Create `handle_value_builder` callback (or feature-flag the existing one)
- Wire it up in `parse_data` instead of the old `handle_value`
- Benchmark to confirm improvement

### Phase 3: Clean up
- Remove `cols: Vec<Vec<ReadStatVar>>` from `ReadStatData`
- Remove old `allocate_cols`
- Simplify `cols_to_batch` to use builders
- Remove `ReadStatVar::get_readstat_value()` if no longer needed elsewhere
- Update all call sites and tests

## Expected Impact

### Memory
- **Before:** 159M String allocations (~8 bytes avg each) = ~1.3 GB heap churn per AHS pass
- **After:** ~2,512 StringBuilder buffers with pre-sized contiguous storage = ~200 MB total
  (2,512 cols × 10,000 rows/chunk × 8 bytes avg = 200 MB, reused per chunk)
- Eliminates `ReadStatVar` enum overhead (24 bytes per cell × 336M cells = ~8 GB)

### Speed
- **Before:** Parse phase = 35s (dominated by per-cell malloc/free)
- **Expected:** 5-10x reduction in parse time for string-heavy datasets
  - `CStr::from_ptr` + `to_str()` + memcpy into StringBuilder vs. `CStr` + `to_string()` (heap alloc)
  - Batch memcpy into pre-sized buffer is much faster than 159M individual allocations
- **Convert phase:** Drops from 2.8s to ~0 (builder.finish() is O(1))
- **Total expected:** 35s + 2.8s → ~5-10s (rough estimate)

### Risks
- `handle_value` becomes more complex (type dispatch + builder access)
- Must ensure builder type matches what metadata says (type mismatch = panic)
- Column filter logic must work with builders (skip columns not in filter)
- `lexical::parse` formatting for doubles still runs per-cell (separate optimization)
- Error handling changes: currently errors are collected; with builders, partial data
  in the builder may need rollback or the row should be skipped

## Files to Modify

1. `crates/readstat/src/rs_data.rs` — struct, allocate_builders, cols_to_batch
2. `crates/readstat/src/cb.rs` — handle_value rewrite
3. `crates/readstat/src/rs_var.rs` — keep for API compatibility, remove from hot path
4. `crates/readstat/src/common.rs` — ptr_to_string remains for non-hot-path uses
5. `crates/readstat/src/lib.rs` — export ColumnBuilder if needed
6. `crates/readstat-tests/tests/string_alloc_bench.rs` — update benchmark
7. All test files that construct `ReadStatData` directly (if any)

## Benchmarking Plan

### Before starting implementation: capture baselines

Run each benchmark 3 times and record the median. All benchmarks use `--release`.

#### 1. End-to-end AHS benchmark (existing)
```bash
cargo test -p readstat-tests --release bench_ahs_string_allocation -- --nocapture --ignored
```
Record: Parse time, Convert time, Total time

#### 2. Small file regression check (existing)
```bash
cargo test -p readstat-tests --release bench_cars_string_allocation -- --nocapture
```
Record: Parse time, Convert time (ensure no regression on small files)

#### 3. Micro-benchmark (existing)
```bash
cargo test -p readstat-tests --release micro_bench_readstatvar_clone_vs_drain -- --nocapture --ignored
```
Record: All four approach times (this becomes obsolete after the change since
ReadStatVar is no longer used in the hot path — document as "N/A: eliminated")

### After implementation: measure improvements

Run the same benchmarks and compare:

| Metric | Before | After | Speedup |
|--------|--------|-------|---------|
| AHS Parse phase | 35s | ? | target 5-10x |
| AHS Convert phase | 2.8s | ? | target ~0s |
| AHS Total | ~38s | ? | target 3-5x |
| Cars Parse | ? | ? | no regression |
| Cars Convert | ? | ? | no regression |

### New benchmark to add

Add a `bench_ahs_builder` test that measures:
1. `allocate_builders` time (should be fast — just builder construction)
2. `parse_data` time (the main target — now includes direct builder appends)
3. `cols_to_batch` time (should be near-zero — just `finish()` calls)
4. Total throughput in rows/sec and MB/sec

### Memory profiling (optional but valuable)

On Linux/WSL, measure peak RSS:
```bash
/usr/bin/time -v cargo test -p readstat-tests --release bench_ahs_string_allocation -- --nocapture --ignored 2>&1 | grep "Maximum resident"
```
Compare before/after to confirm reduced heap churn.
