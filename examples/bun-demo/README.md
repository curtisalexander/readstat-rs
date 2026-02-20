# readstat-wasm Bun Demo

Demonstrates reading SAS `.sas7bdat` file metadata from JavaScript using the `readstat-wasm` package compiled to WebAssembly via Emscripten.

## 1. Install dependencies

### Rust + wasm target

```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Add the Emscripten wasm target
rustup target add wasm32-unknown-emscripten
```

### Emscripten SDK

```bash
# Clone the SDK
git clone https://github.com/emscripten-core/emsdk.git
cd emsdk

# Install and activate the latest toolchain
./emsdk install latest
./emsdk activate latest

# Add to your current shell (run this every new terminal session,
# or add it to your shell profile)
source ./emsdk_env.sh
```

### libclang (required by bindgen)

```bash
# macOS
brew install llvm

# Ubuntu / Debian
sudo apt-get install libclang-dev

# Fedora
sudo dnf install clang-devel
```

### Bun

```bash
curl -fsSL https://bun.sh/install | bash
```

## 2. Initialize git submodules

From the repository root:

```bash
git submodule update --init --recursive
```

## 3. Build the WASM package

```bash
# Make sure Emscripten is activated in your shell
source /path/to/emsdk/emsdk_env.sh

# Build the Emscripten zlib port (first time only — takes ~2 seconds)
embuilder build zlib

# From the readstat-wasm crate directory
cd crates/readstat-wasm

# Build with Emscripten target (release mode)
cargo build --target wasm32-unknown-emscripten --release

# Copy the .wasm binary into the pkg/ directory
cp target/wasm32-unknown-emscripten/release/readstat_wasm.wasm pkg/
```

## 4. Run the demo

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
  [3] Wagon (Double, )
  [4] Pickup (Double, )
  [5] Automatic (Double, )
  [6] EngineSize (Double, )
  [7] Cylinders (Double, )
  [8] CityMPG (Double, )
  [9] HwyMPG (Double, )
  [10] SUV (Double, )
  [11] AWD (Double, )
  [12] Hybrid (Double, )
```

## How it works

The `readstat-wasm` crate compiles the ReadStat C library and the Rust `readstat` parsing library to WebAssembly using the `wasm32-unknown-emscripten` target. Emscripten is required because the underlying ReadStat C code needs a C standard library (libc, zlib, iconv) — which Emscripten provides for wasm.

The crate exports three C-compatible functions:

| Export | Signature | Purpose |
|--------|-----------|---------|
| `read_metadata` | `(ptr, len) -> *char` | Parse metadata from a byte buffer |
| `read_metadata_fast` | `(ptr, len) -> *char` | Same, but skips full row count |
| `free_string` | `(ptr)` | Free a string returned by the above |

The JS wrapper in `pkg/readstat_wasm.js` handles:
- Loading the `.wasm` module
- Providing minimal WASI and Emscripten import stubs
- Memory management (malloc/free for input bytes, free_string for output)
- Converting between JS types and wasm pointers

## Troubleshooting

**`zlib.h: file not found` during build**
Run `embuilder build zlib` to install the Emscripten zlib port.

**`EMSDK must be set for Emscripten builds`**
Run `source /path/to/emsdk/emsdk_env.sh` to activate the Emscripten SDK in your shell.

**`error: linking with emcc failed` / `undefined symbol: main`**
Make sure you're building from `crates/readstat-wasm/` (not the repo root). The `.cargo/config.toml` in that directory provides the necessary linker flags.
