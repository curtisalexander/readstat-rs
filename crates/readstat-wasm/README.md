# readstat-wasm

WebAssembly build of the `readstat` library for parsing SAS `.sas7bdat` files in JavaScript. Reads metadata and converts row data to CSV or NDJSON entirely in memory — no server or native dependencies required at runtime.

## Package contents

The `pkg/` directory contains everything needed to use the library from JavaScript:

| File | Description |
|------|-------------|
| `readstat_wasm.wasm` | Pre-built WASM binary (Emscripten target) |
| `readstat_wasm.js` | JS wrapper handling module loading, memory management, and type conversion |

## JS API

All functions accept a `Uint8Array` of raw `.sas7bdat` file bytes.

```js
import { init, read_metadata, read_metadata_fast, read_data, read_data_ndjson } from "readstat-wasm";

// Must be called once before using any other function
await init();

const bytes = new Uint8Array(/* .sas7bdat file contents */);

// Metadata (returns JSON string)
const metadataJson = read_metadata(bytes);
const metadataJsonFast = read_metadata_fast(bytes); // skips full row count

// Data (returns string)
const csv = read_data(bytes);       // CSV with header row
const ndjson = read_data_ndjson(bytes); // newline-delimited JSON
```

### Functions

| Function | Returns | Description |
|----------|---------|-------------|
| `init()` | `Promise<void>` | Load and initialize the WASM module |
| `read_metadata(bytes)` | `string` | File and variable metadata as JSON |
| `read_metadata_fast(bytes)` | `string` | Same as above but skips full row count for speed |
| `read_data(bytes)` | `string` | All row data as CSV (with header) |
| `read_data_ndjson(bytes)` | `string` | All row data as newline-delimited JSON |

## How it works

The crate compiles the [ReadStat](https://github.com/WizardMac/ReadStat) C library and the Rust `readstat` parsing library to WebAssembly using the `wasm32-unknown-emscripten` target. Emscripten is required because the underlying C code needs a C standard library (libc, iconv).

The data functions perform a two-pass parse over the byte buffer: first to extract metadata (schema, row count), then to read row values into an Arrow `RecordBatch`, which is serialized to CSV or NDJSON in memory.

### C ABI exports

The WASM module exposes these C-compatible functions (used internally by the JS wrapper):

| Export | Signature | Purpose |
|--------|-----------|---------|
| `read_metadata` | `(ptr, len) -> *char` | Parse metadata as JSON |
| `read_metadata_fast` | `(ptr, len) -> *char` | Same, skipping full row count |
| `read_data` | `(ptr, len) -> *char` | Parse data, return as CSV |
| `read_data_ndjson` | `(ptr, len) -> *char` | Parse data, return as NDJSON |
| `free_string` | `(ptr)` | Free a string returned by the above |

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
cargo build --target wasm32-unknown-emscripten --release

# Copy binary to pkg/
cp target/wasm32-unknown-emscripten/release/readstat_wasm.wasm pkg/
```

See the [bun-demo](../../examples/bun-demo/) for a working example.
