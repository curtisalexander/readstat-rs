[< Back to README](../README.md)

# Visual Guide: From SAS Bytes to Arrow and Writers

This guide is a map for contributors and advanced users who want to understand what happens between opening a `.sas7bdat` file and writing CSV, Parquet, Feather, or NDJSON.

## The Pipeline at a Glance

```text
┌──────────────────────┐
│ Path / bytes / mmap  │
│ .sas7bdat input      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ ReadStatParser       │  RAII wrapper around readstat_parser_t
│ - handlers           │  registers Rust callbacks with C
│ - row limit/offset   │
│ - optional IO ctx    │
└──────────┬───────────┘
           │ calls C ReadStat parser
           ▼
┌────────────────────────────────────────────────────────────┐
│ Metadata pass                                              │
│                                                            │
│ handle_metadata(ctx = &mut ReadStatMetadata)               │
│   └─ row_count, var_count, label, encoding, compression    │
│                                                            │
│ handle_variable(ctx = &mut ReadStatMetadata)               │
│   └─ ReadStatVarMetadata per variable                      │
│      name, label, type, format, storage/display widths     │
│                                                            │
│ ReadStatMetadata::initialize_schema()                      │
│   └─ Arrow Schema + field metadata                         │
└──────────┬─────────────────────────────────────────────────┘
           │ metadata drives data builders
           ▼
┌────────────────────────────────────────────────────────────┐
│ Data pass                                                  │
│                                                            │
│ ReadStatData::init(md, row_start, row_end)                 │
│   └─ allocates one ColumnBuilder per selected variable     │
│                                                            │
│ handle_value(ctx = &mut ReadStatData)                      │
│   ├─ checks missingness and ReadStat value type            │
│   ├─ converts SAS date/time/datetime values                │
│   ├─ appends directly to typed Arrow builders              │
│   └─ tracks row boundaries/progress                        │
│                                                            │
│ ReadStatData::cols_to_batch()                              │
│   └─ finishes builders into an Arrow RecordBatch           │
└──────────┬─────────────────────────────────────────────────┘
           │
           ▼
┌────────────────────────────────────────────────────────────┐
│ Arrow RecordBatch                                          │
│ - typed arrays                                             │
│ - schema and field metadata                                │
│ - ready for Rust callers, DataFusion SQL, or writers       │
└──────────┬─────────────────────────────────────────────────┘
           │
           ▼
┌────────────────────────────────────────────────────────────┐
│ Writers                                                    │
│ ReadStatWriter / write_batch_to_*_bytes                    │
│ - CSV                                                      │
│ - Feather (Arrow IPC)                                      │
│ - NDJSON                                                   │
│ - Parquet                                                  │
└────────────────────────────────────────────────────────────┘
```

## Two Passes Are Intentional

`readstat` first reads metadata, then reads data. That may look redundant, but it buys several important properties:

1. **Correct Arrow types before values arrive** — SAS numeric values can represent plain numbers, dates, times, or datetimes depending on the SAS format string.
2. **Direct builder appends** — once the schema is known, values can go straight into typed Arrow builders instead of an intermediate row model.
3. **Column projection** — selected columns can be resolved by name before the data pass allocates builders.
4. **Streaming chunks** — row offsets and row limits let the CLI and library parse large files in bounded chunks.

## Context Pointers Across the FFI Boundary

The C parser accepts a raw `void*` user context. The safe Rust-facing flow is:

```text
&mut ReadStatMetadata ──cast──► *mut c_void ──C callback──► &mut ReadStatMetadata
&mut ReadStatData     ──cast──► *mut c_void ──C callback──► &mut ReadStatData
```

The callback functions in `crates/readstat/src/cb.rs` are the only places where row and metadata values cross from C into Rust-owned data structures. They must never panic across the `extern "C"` boundary; recoverable callback failures are stored in `ReadStatData::abort_error` and surfaced as `ReadStatError` after parsing returns.

## How a Cell Becomes Arrow Data

```text
ReadStat C value
  ├─ variable index + ReadStat type
  ├─ missingness flag
  └─ raw value accessor
       │
       ▼
handle_value
  ├─ skip if column filter excludes this variable
  ├─ append null if SAS says missing
  ├─ convert strings lossy only when not valid UTF-8
  ├─ round numeric values to avoid SAS/IEEE display noise
  ├─ convert SAS epoch date/time values to Arrow epoch units
  └─ append to ColumnBuilder variant
       │
       ▼
Arrow ArrayBuilder
       │ finish()
       ▼
Arrow ArrayRef in RecordBatch
```

## Contributor Landmarks

| Concern | Start here |
|---------|------------|
| High-level one-call API | `crates/readstat/src/api.rs` |
| Public exports and crate docs | `crates/readstat/src/lib.rs` |
| Metadata extraction | `crates/readstat/src/rs_metadata.rs` and `cb.rs::handle_metadata/handle_variable` |
| Data extraction | `crates/readstat/src/rs_data.rs` and `cb.rs::handle_value` |
| FFI parser lifecycle | `crates/readstat/src/rs_parser.rs` |
| Bytes / mmap IO | `crates/readstat/src/rs_buffer_io.rs` |
| Output formats | `crates/readstat/src/rs_write.rs` |
| CLI orchestration | `crates/readstat-cli/src/main.rs` |
| Safety checks | `docs/MEMORY-SAFETY.md` |

## API Layers

Use the highest layer that matches your task:

```rust,no_run
# fn main() -> Result<(), readstat::ReadStatError> {
// 1. Simple whole-file read
let batch = readstat::read_to_batch("data.sas7bdat")?;

// 2. Common options: projection and row ranges
let preview = readstat::read_to_batch_with_options(
    "data.sas7bdat",
    readstat::ReadOptions::new()
        .columns(["name", "age"])
        .row_count(100),
)?;

// 3. Low-level streaming / writer orchestration
let rsp = readstat::ReadStatPath::new("data.sas7bdat")?;
let mut md = readstat::ReadStatMetadata::new();
md.read_metadata(&rsp, false)?;
let mut chunk = readstat::ReadStatData::new().init(md, 0, 10_000);
chunk.read_data(&rsp)?;
# Ok(())
# }
```

## Where Polish Would Help Next

- Add a true `RecordBatch` iterator/streaming API so callers do not need to manually juggle offsets.
- Keep tightening public fields into methods over time; much of the current low-level state is exposed for historical convenience.
- Add architecture diagrams to generated rustdocs once the book and docs.rs flows settle.
