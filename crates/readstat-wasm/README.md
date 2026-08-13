# readstat-wasm

WebAssembly build of the `readstat` library for parsing SAS `.sas7bdat` files in JavaScript. Reads metadata and converts row data to CSV, NDJSON, Parquet, or Feather (Arrow IPC) entirely in memory — no server or native dependencies required at runtime.

## Package contents

The `pkg/` directory contains everything needed to use the library from JavaScript:

| File | Description |
|------|-------------|
| `readstat_wasm.wasm` | Pre-built WASM binary (Emscripten target) |
| `readstat_wasm.js` | JS wrapper handling module loading, memory management, and type conversion |
| `package.json` | Package identity and version, kept in lockstep with the Rust crate |

Versioned bundles containing these three files are attached to GitHub Releases.

## JS API

All functions accept a `Uint8Array` of raw `.sas7bdat` file bytes.

```js
import { init, read_metadata, read_metadata_fast, read_preview, read_data, read_data_reduced, read_data_ndjson, read_data_ndjson_reduced, read_data_parquet, read_data_parquet_reduced, read_data_feather, read_data_feather_reduced, read_data_arrow_stream_reduced, create_arrow_stream_session, read_arrow_stream_session_batch, free_arrow_stream_session } from "readstat-wasm";

// Must be called once before using any other function
await init();

const bytes = new Uint8Array(/* .sas7bdat file contents */);

// Metadata (returns JSON string)
const metadataJson = read_metadata(bytes);
const metadataJsonFast = read_metadata_fast(bytes); // skips full row count
const previewNdjson = read_preview(bytes, 100); // parses at most 100 rows

// Data as text (returns string)
const csv = read_data(bytes);       // CSV with header row
const ndjson = read_data_ndjson(bytes); // newline-delimited JSON

// Data as binary (returns Uint8Array)
const parquet = read_data_parquet(bytes);  // Parquet bytes
const feather = read_data_feather(bytes);  // Feather (Arrow IPC) bytes

// Selected columns and a bounded, zero-based row range
const selection = { columns: ["name", "age"], rowOffset: 100, rowLimit: 50 };
const reducedCsv = read_data_reduced(bytes, selection);
const reducedNdjson = read_data_ndjson_reduced(bytes, selection);
const reducedParquet = read_data_parquet_reduced(bytes, selection);
const reducedFeather = read_data_feather_reduced(bytes, selection);
const reducedArrowStream = read_data_arrow_stream_reduced(bytes, selection);

// Retain one input copy and resolved schema across multiple bounded reads.
const session = create_arrow_stream_session(bytes, selection.columns);
try {
  const firstBatch = read_arrow_stream_session_batch(
    session,
    selection.rowOffset,
    selection.rowLimit,
  );
} finally {
  free_arrow_stream_session(session);
}
```

### Functions

| Function | Returns | Description |
|----------|---------|-------------|
| `init()` | `Promise<void>` | Load and initialize the WASM module |
| `read_metadata(bytes)` | `string` | File and variable metadata as JSON |
| `read_metadata_fast(bytes)` | `string` | Same as above but skips full row count for speed |
| `read_preview(bytes, rowLimit)` | `string` | At most `rowLimit` rows as NDJSON |
| `read_data(bytes)` | `string` | All row data as CSV (with header) |
| `read_data_ndjson(bytes)` | `string` | All row data as newline-delimited JSON |
| `read_data_parquet(bytes)` | `Uint8Array` | All row data as Parquet bytes |
| `read_data_feather(bytes)` | `Uint8Array` | All row data as Feather (Arrow IPC) bytes |
| `read_data_reduced(bytes, selection)` | `string` | Selected columns and bounded rows as CSV |
| `read_data_ndjson_reduced(bytes, selection)` | `string` | Selected columns and bounded rows as NDJSON |
| `read_data_parquet_reduced(bytes, selection)` | `Uint8Array` | Selected columns and bounded rows as Parquet |
| `read_data_feather_reduced(bytes, selection)` | `Uint8Array` | Selected columns and bounded rows as Feather |
| `read_data_arrow_stream_reduced(bytes, selection)` | `Uint8Array` | Selected columns and bounded rows as an Arrow IPC stream |
| `create_arrow_stream_session(bytes, columns)` | `number` | Retain one input copy and selected schema for bounded Arrow IPC reads |
| `read_arrow_stream_session_batch(handle, rowOffset, rowLimit)` | `Uint8Array` | Read one bounded Arrow IPC stream from a session |
| `free_arrow_stream_session(handle)` | `void` | Release a session and its retained input |

Reduced exports require at least one column, a `rowOffset` from 0 through
4,294,967,295, and a `rowLimit` from 1 through 4,294,967,295. Column order in
the output follows dataset order, regardless of the order in `selection.columns`.

## How it works

The crate compiles the [ReadStat](https://github.com/WizardMac/ReadStat) C library and the Rust `readstat` parsing library to WebAssembly using the `wasm32-unknown-emscripten` target. Emscripten is required because the underlying C code needs a C standard library (libc, iconv).

The data functions perform a two-pass parse over the byte buffer: first to extract metadata (schema, row count), then to read row values into an Arrow `RecordBatch`, which is serialized to CSV, NDJSON, Parquet, or Feather in memory.

### C ABI exports

The WASM module exposes these C-compatible functions (used internally by the JS wrapper):

| Export | Signature | Purpose |
|--------|-----------|---------|
| `read_metadata` | `(ptr, len) -> *char` | Parse metadata as JSON |
| `read_metadata_fast` | `(ptr, len) -> *char` | Same, skipping full row count |
| `read_preview` | `(ptr, len, row_limit) -> *char` | Parse at most `row_limit` rows as NDJSON |
| `read_data` | `(ptr, len) -> *char` | Parse data, return as CSV |
| `read_data_ndjson` | `(ptr, len) -> *char` | Parse data, return as NDJSON |
| `read_data_parquet` | `(ptr, len, out_len) -> *u8` | Parse data, return as Parquet bytes |
| `read_data_feather` | `(ptr, len, out_len) -> *u8` | Parse data, return as Feather bytes |
| `read_data_reduced` | `(ptr, len, columns_ptr, columns_len, row_offset, row_limit) -> *char` | Parse selected rows/columns as CSV |
| `read_data_ndjson_reduced` | `(ptr, len, columns_ptr, columns_len, row_offset, row_limit) -> *char` | Parse selected rows/columns as NDJSON |
| `read_data_parquet_reduced` | `(ptr, len, columns_ptr, columns_len, row_offset, row_limit, out_len) -> *u8` | Parse selected rows/columns as Parquet |
| `read_data_feather_reduced` | `(ptr, len, columns_ptr, columns_len, row_offset, row_limit, out_len) -> *u8` | Parse selected rows/columns as Feather |
| `read_data_arrow_stream_reduced` | `(ptr, len, columns_ptr, columns_len, row_offset, row_limit, out_len) -> *u8` | Parse selected rows/columns as an Arrow IPC stream |
| `create_arrow_stream_session` | `(ptr, len, columns_ptr, columns_len) -> u32` | Retain an input copy and selected schema, returning a session handle |
| `read_arrow_stream_session_batch` | `(handle, row_offset, row_limit, out_len) -> *u8` | Parse one bounded Arrow IPC stream from a session |
| `free_arrow_stream_session` | `(handle)` | Release a session and its retained input |
| `readstat_last_error` | `() -> *char` | Borrow the last native error for the current thread |
| `free_string` | `(ptr)` | Free a string returned by the above |
| `free_binary` | `(ptr, len)` | Free a binary buffer returned by a binary data export |

The reduced C exports accept `columns_ptr` as a UTF-8 JSON array of column
names. Read functions return null on failure. `readstat_last_error` then returns an
actionable borrowed message, valid until the next read call on that thread. The
caller must not free that pointer. The JavaScript wrapper converts it to an
`Error` automatically.

Emscripten browser hosts must provide an `env.readstat_progress(stage, current,
total)` import. The package wrapper accepts `init({ onProgress })` and supplies
that import automatically. Stage values are 1 metadata, 2 preview parsing, 3
preview encoding, 4 export parsing, and 5 export encoding. A total of zero means
the stage has no determinate percentage.

## Building from source

Requires Rust, Emscripten SDK, and libclang.

```bash
# Activate Emscripten
source /path/to/emsdk/emsdk_env.sh

# Add the target (first time only)
rustup target add wasm32-unknown-emscripten

# Initialize submodules (first time only, from repo root)
git submodule update --init --recursive

# Build
cargo build --locked --target wasm32-unknown-emscripten --release

# Copy binary to pkg/
cp target/wasm32-unknown-emscripten/release/readstat_wasm.wasm pkg/
```

See the [bun-demo](../../examples/bun-demo/) for a working example.
