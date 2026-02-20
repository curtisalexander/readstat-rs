# readstat-wasm Bun Demo

Demonstrates reading SAS `.sas7bdat` file metadata from JavaScript using the `readstat-wasm` package.

## Prerequisites

- [Bun](https://bun.sh/) runtime
- [Emscripten SDK](https://emscripten.org/docs/getting_started/downloads.html) (for building the WASM package)
- [wasm-bindgen-cli](https://rustwasm.github.io/wasm-bindgen/reference/cli.html) (`cargo install wasm-bindgen-cli`)

## Build the WASM package

```bash
# From the repository root
cd crates/readstat-wasm

# Build with Emscripten target
cargo build --target wasm32-unknown-emscripten --release

# Generate JS/TS bindings
wasm-bindgen \
  ../../target/wasm32-unknown-emscripten/release/readstat_wasm.wasm \
  --out-dir pkg \
  --target web
```

Alternatively, if `wasm-pack` works with C FFI:

```bash
wasm-pack build --target web
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
File encoding: UTF-8
Row count:     428
Variable count:15
...

=== Variables ===
  [0] Make (String, $40)
  [1] Model (String, $40)
  ...
```
