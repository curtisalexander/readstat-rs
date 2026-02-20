# readstat-wasm Bun Demo

Demonstrates reading SAS `.sas7bdat` file metadata and data from JavaScript using the `readstat-wasm` package compiled to WebAssembly via Emscripten. The demo parses a `.sas7bdat` file entirely in-memory via WASM and converts it to CSV.

## Quick start

If you already have Rust, Emscripten SDK, libclang, and Bun installed:

**macOS / Linux:**

```bash
# Activate Emscripten (first time per terminal session)
source /path/to/emsdk/emsdk_env.sh

# Add the wasm target (first time only)
rustup target add wasm32-unknown-emscripten

# Initialize submodules (first time only)
git submodule update --init --recursive

# Build the wasm package
cd crates/readstat-wasm
cargo build --target wasm32-unknown-emscripten --release
cp target/wasm32-unknown-emscripten/release/readstat_wasm.wasm pkg/

# Run the demo
cd ../../examples/bun-demo
bun install
bun run index.ts
```

**Windows (Git Bash):**

```bash
# Activate Emscripten (first time per terminal session)
/c/path/to/emsdk/emsdk.bat activate latest
export EMSDK=C:/path/to/emsdk

# Add the wasm target (first time only)
rustup target add wasm32-unknown-emscripten

# Initialize submodules (first time only)
git submodule update --init --recursive

# Build the wasm package
cd crates/readstat-wasm
cargo build --target wasm32-unknown-emscripten --release
cp target/wasm32-unknown-emscripten/release/readstat_wasm.wasm pkg/

# Run the demo
cd ../../examples/bun-demo
bun install
bun run index.ts
```

**Windows (PowerShell):**

```powershell
# Activate Emscripten (first time per terminal session)
C:\path\to\emsdk\emsdk.bat activate latest
$env:EMSDK = "C:\path\to\emsdk"

# Add the wasm target (first time only)
rustup target add wasm32-unknown-emscripten

# Initialize submodules (first time only)
git submodule update --init --recursive

# Build the wasm package
cd crates\readstat-wasm
cargo build --target wasm32-unknown-emscripten --release
copy target\wasm32-unknown-emscripten\release\readstat_wasm.wasm pkg\

# Run the demo
cd ..\..\examples\bun-demo
bun install
bun run index.ts
```

## 1. Install dependencies

### Rust + wasm target

```bash
# Install Rust (if not already installed)
# macOS / Linux
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Windows — download and run rustup-init.exe from https://rustup.rs

# Add the Emscripten wasm target (all platforms)
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
```

Activate in your shell (run every new terminal session, or add to your profile):

| Platform | Command |
|----------|---------|
| macOS / Linux | `source ./emsdk_env.sh` |
| Windows (cmd) | `emsdk_env.bat` |
| Windows (PowerShell) | `emsdk_env.bat` (then set `$env:EMSDK = "C:\path\to\emsdk"` if needed) |
| Windows (Git Bash) | `source ./emsdk_env.sh` (then `export EMSDK=C:/path/to/emsdk` if needed) |

> **Note:** On Windows, `emsdk_env.sh` / `emsdk_env.bat` may update PATH without
> exporting the `EMSDK` variable. If the build fails with "EMSDK must be set",
> set it manually as shown above. The build script will also attempt to auto-detect
> the emsdk root from PATH.

### libclang (required by bindgen)

| Platform | Command |
|----------|---------|
| macOS | `brew install llvm` |
| Ubuntu / Debian | `sudo apt-get install libclang-dev` |
| Fedora | `sudo dnf install clang-devel` |
| Windows | Install LLVM from https://releases.llvm.org/download.html and set `LIBCLANG_PATH` to the `lib` directory (e.g., `C:\Program Files\LLVM\lib`) |

### Bun

```bash
# macOS / Linux
curl -fsSL https://bun.sh/install | bash

# Windows (PowerShell)
powershell -c "irm bun.sh/install.ps1 | iex"
```

## 2. Initialize git submodules

From the repository root:

```bash
git submodule update --init --recursive
```

## 3. Build the WASM package

```bash
# Make sure Emscripten is activated in your shell (see table above)

# From the readstat-wasm crate directory
cd crates/readstat-wasm

# Build with Emscripten target (release mode)
cargo build --target wasm32-unknown-emscripten --release

# Copy the .wasm binary into the pkg/ directory
# macOS / Linux
cp target/wasm32-unknown-emscripten/release/readstat_wasm.wasm pkg/
# Windows (PowerShell)
# copy target\wasm32-unknown-emscripten\release\readstat_wasm.wasm pkg\
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

=== CSV Data (preview) ===
Brand,Model,Minivan,Wagon,Pickup,Automatic,EngineSize,Cylinders,CityMPG,HwyMPG,SUV,AWD,Hybrid
TOYOTA,Prius,0.0,0.0,0.0,1.0,1.5,4.0,60.0,51.0,0.0,0.0,1.0
HONDA,Civic Hybrid,0.0,0.0,0.0,1.0,1.3,4.0,48.0,47.0,0.0,0.0,1.0
HONDA,Civic Hybrid,0.0,0.0,0.0,1.0,1.3,4.0,47.0,48.0,0.0,0.0,1.0
HONDA,Civic Hybrid,0.0,0.0,0.0,0.0,1.3,4.0,46.0,51.0,0.0,0.0,1.0
HONDA,Civic Hybrid,0.0,0.0,0.0,0.0,1.3,4.0,45.0,51.0,0.0,0.0,1.0
... (1081 total data rows)

Wrote 1081 rows to cars.csv
```

## How it works

The `readstat-wasm` crate compiles the ReadStat C library and the Rust `readstat` parsing library to WebAssembly using the `wasm32-unknown-emscripten` target. Emscripten is required because the underlying ReadStat C code needs a C standard library (libc, iconv) — which Emscripten provides for wasm. (Note: zlib is only needed for SPSS zsav support, which is not included in the current wasm build.)

The crate exports five C-compatible functions:

| Export | Signature | Purpose |
|--------|-----------|---------|
| `read_metadata` | `(ptr, len) -> *char` | Parse metadata as JSON from a byte buffer |
| `read_metadata_fast` | `(ptr, len) -> *char` | Same, but skips full row count |
| `read_data` | `(ptr, len) -> *char` | Parse data and return as CSV string |
| `read_data_ndjson` | `(ptr, len) -> *char` | Parse data and return as NDJSON string |
| `free_string` | `(ptr)` | Free a string returned by the above |

The data functions perform a two-pass parse over the same byte buffer: first to extract metadata (schema, row count), then to read row values into an Arrow `RecordBatch`, which is serialized to CSV or NDJSON in memory.

The JS wrapper in `pkg/readstat_wasm.js` handles:
- Loading the `.wasm` module
- Providing minimal WASI and Emscripten import stubs
- Memory management (malloc/free for input bytes, free_string for output)
- Converting between JS types and wasm pointers

## Troubleshooting

**`EMSDK must be set for Emscripten builds`**
Set the `EMSDK` environment variable to point to your emsdk installation directory. On macOS/Linux: `export EMSDK=/path/to/emsdk`. On Windows (PowerShell): `$env:EMSDK = "C:\path\to\emsdk"`. On Windows (Git Bash): `export EMSDK=C:/path/to/emsdk`. The build script also attempts to auto-detect the emsdk root from your PATH, so simply having Emscripten activated may be sufficient.

**`error: linking with emcc failed` / `undefined symbol: main`**
Make sure you're building from `crates/readstat-wasm/` (not the repo root). The `.cargo/config.toml` in that directory provides the necessary linker flags.

**`The command line is too long` (Windows)**
This was a known issue when building all ReadStat C source files for the Emscripten target. It has been fixed — the build script now compiles only the SAS format sources for Emscripten builds, keeping the archiver command within Windows' command-line length limit.
