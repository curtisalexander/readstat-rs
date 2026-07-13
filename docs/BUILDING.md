[< Back to README](../README.md)

# Building from Source

## Minimum Supported Rust Version (MSRV)

All published crates require **Rust `1.88`** or newer (let-chains; Rust edition 2024), as declared by `rust-version = "1.88"` in each crate's `Cargo.toml`.

## Clone
Ensure submodules are also cloned.

```sh
git clone --recurse-submodules https://github.com/curtisalexander/readstat-rs.git
```

The [ReadStat](https://github.com/WizardMac/ReadStat) repository is included as a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules) within this repository.  In order to build and link, first a [readstat-sys](https://github.com/curtisalexander/readstat-rs/tree/main/crates/readstat-sys) crate is created.  Then the [readstat](https://github.com/curtisalexander/readstat-rs/tree/main/crates/readstat) library and [readstat-cli](https://github.com/curtisalexander/readstat-rs/tree/main/crates/readstat-cli) binary crate utilize `readstat-sys` as a dependency.

## Linux
Install developer tools

```sh
sudo apt install build-essential
```

Build
```sh
cargo build
```

**iconv**: Linked dynamically against the system-provided library. On most distributions it is available by default. No explicit link directives are emitted in the build script &mdash; the system linker resolves it automatically.

**zlib**: Linked via the [libz-sys](https://crates.io/crates/libz-sys) crate, which will use the system-provided zlib if available or compile from source as a fallback.

## macOS
Install developer tools

```sh
xcode-select --install
```

Build
```sh
cargo build
```

**iconv**: Linked dynamically against the system-provided library that ships with macOS (via `cargo:rustc-link-lib=iconv` in the [readstat-sys build script](../crates/readstat-sys/build.rs)). No additional packages need to be installed.

**zlib**: Linked via the [libz-sys](https://crates.io/crates/libz-sys) crate, which will use the system-provided zlib that ships with macOS.

## Windows
Building on Windows requires [Visual Studio C++ Build tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) be installed.

Build
```sh
cargo build
```

**iconv**: Compiled from source using the vendored [win-iconv](https://github.com/win-iconv/win-iconv) submodule (located at `crates/readstat-iconv-sys/vendor/win-iconv/`; public domain, so static linking carries no copyleft obligations) via the [readstat-iconv-sys](../crates/readstat-iconv-sys/) crate. `readstat-iconv-sys` is a Windows-only dependency (gated behind `[target.'cfg(windows)'.dependencies]` in [readstat-sys/Cargo.toml](../crates/readstat-sys/Cargo.toml)).

**zlib**: Compiled from source via the [libz-sys](https://crates.io/crates/libz-sys) crate (statically linked).

### Windows (GNU / MinGW)

The `x86_64-pc-windows-gnu` target is also supported, with its own pre-generated
bindings (`bindings_windows_gnu_x86_64.rs` — the MSVC and GNU ABIs differ in C
enum signedness, so the flavors cannot share a file). It needs a MinGW-w64 GCC
for the vendored C code:

- **On Windows**: `pacman -S mingw-w64-x86_64-gcc` in MSYS2, with
  `C:\msys64\mingw64\bin` on `PATH`, then
  `cargo build --target x86_64-pc-windows-gnu`.
- **Cross-compiling from Linux**: install `gcc-mingw-w64-x86-64` (Debian/Ubuntu)
  and run `cargo build --target x86_64-pc-windows-gnu` — this links a complete
  `readstat.exe` with no Windows machine involved.

## Regenerating bindings (maintainers only)

Default builds consume pre-generated bindings checked into `crates/readstat-sys/src/bindings/bindings_<os>_<arch>.rs`, so no `libclang` / LLVM install is required. If you need to regenerate the bindings (e.g. after bumping the vendored ReadStat sources or changing `wrapper.h`), enable the `buildtime_bindgen` feature on `readstat-sys`:

```sh
READSTAT_REGEN_BINDINGS=1 cargo build -p readstat-sys --features buildtime_bindgen
```

This invokes [bindgen](https://rust-lang.github.io/rust-bindgen/), which requires [LLVM / `libclang`](https://rust-lang.github.io/rust-bindgen/requirements.html#clang) to be installed. On Windows specifically, you also need to set `LIBCLANG_PATH` (e.g. `C:\Program Files\LLVM\lib`). The build script always writes the regenerated file to `OUT_DIR` (for the current compile); setting `READSTAT_REGEN_BINDINGS=1` additionally refreshes the target's checked-in file under `src/bindings/` (`bindings_<os>_<arch>.rs`; the `x86_64-pc-windows-gnu` target uses `bindings_windows_gnu_x86_64.rs`), so the diff can be committed. Without the env var the feature never touches committed files — so a workspace-wide `--all-features` build can't silently dirty the bindings. Regeneration must be repeated on each supported target — the **readstat-sys cross-platform CI** workflow (`regen` jobs) can do this for you (`workflow_dispatch` → download artifacts → commit); see [CI-CD.md](CI-CD.md) for the full procedure.

`wasm32-unknown-emscripten` builds require `--features buildtime_bindgen` because the emsdk sysroot can't be reproduced from a checked-in file.

## Linking Summary

| Platform | iconv | zlib |
|----------|-------|------|
| Linux (glibc/musl) | Dynamic (system) | libz-sys (prefers system, falls back to source) |
| macOS (x86/ARM) | Dynamic (system) | libz-sys (uses system) |
| Windows (MSVC or GNU) | Static (vendored win-iconv submodule) | libz-sys (compiled from source, static) |
