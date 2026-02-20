# readstat-wasm Bun Demo

Demonstrates reading SAS `.sas7bdat` file metadata from JavaScript using the `readstat-wasm` package.

## Prerequisites

- [Bun](https://bun.sh/) runtime
- [Emscripten SDK](https://emscripten.org/docs/getting_started/downloads.html) (for building the WASM package)
- Rust with the `wasm32-unknown-emscripten` target (`rustup target add wasm32-unknown-emscripten`)

## Build the WASM package

```bash
# Ensure the Emscripten SDK is activated
source /path/to/emsdk/emsdk_env.sh

# Build the zlib port (first time only)
embuilder build zlib

# From the readstat-wasm crate directory
cd crates/readstat-wasm

# Build with Emscripten target
cargo build --target wasm32-unknown-emscripten --release

# Copy the wasm binary into the pkg directory
cp target/wasm32-unknown-emscripten/release/readstat_wasm.wasm pkg/
```

## Run the demo

```bash
cd examples/bun-demo
bun install
bun run index.ts
```

## Expected output

```
=== SAS7BDAT Metadata ===
Table name:    CARS
File encoding: WINDOWS-1252
Row count:     1081
Variable count:13
Compression:   None
Endianness:    Little
Created:       2008-09-30 12:55:01
Modified:      2008-09-30 12:55:01

=== Variables ===
  [0] Brand (String, )
  [1] Model (String, )
  [2] Minivan (Double, )
  ...
```
