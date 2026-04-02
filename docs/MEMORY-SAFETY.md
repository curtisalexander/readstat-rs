[< Back to README](../README.md)

# Memory Safety

This project contains unsafe Rust code (FFI callbacks, pointer casts, memory-mapped I/O) and links against the vendored ReadStat C library. Four automated CI checks guard against memory errors.

## CI Jobs

All four jobs run on every workflow dispatch and tag push, in parallel with the build jobs. Any memory error fails the job with a nonzero exit code.

### Miri (Rust undefined behavior)

- **Platform**: Ubuntu (Linux)
- **Scope**: Unit tests in the `readstat` crate only (`cargo miri test -p readstat`)
- **What it catches**: Undefined behavior in pure-Rust unsafe code — invalid pointer arithmetic, uninitialized reads, provenance violations, use-after-free in Rust allocations
- **Limitation**: Cannot execute FFI calls into C code, so integration tests (`readstat-tests`) are excluded

Configuration:
- Uses Rust nightly with the `miri` component
- `MIRIFLAGS="-Zmiri-disable-isolation"` allows tests that use `tempfile` to create directories

### AddressSanitizer — Linux

- **Platform**: Ubuntu (Linux)
- **Scope**: Full workspace — lib tests, integration tests, binary tests (`cargo test --workspace --lib --tests --bins`)
- **What it catches**: Heap/stack buffer overflows, use-after-free, double-free, memory leaks (LeakSanitizer is enabled by default on Linux), across both Rust and C code

Configuration:
- `RUSTFLAGS="-Zsanitizer=address -Clinker=clang"` — instruments Rust code and links the ASan runtime via clang
- `READSTAT_SANITIZE_ADDRESS=1` — triggers `readstat-sys/build.rs` to compile the ReadStat C library with `-fsanitize=address -fno-omit-frame-pointer`
- Doctests are excluded (`--lib --tests --bins`) because `rustdoc` does not properly inherit sanitizer linker flags

### AddressSanitizer — macOS

- **Platform**: macOS (arm64)
- **Scope**: Full workspace — lib tests, integration tests, binary tests
- **What it catches**: Buffer overflows, use-after-free, double-free in Rust code and at the FFI boundary

Configuration:
- `RUSTFLAGS="-Zsanitizer=address"` — instruments Rust code only
- The ReadStat C library is **not** instrumented on macOS because Apple Clang and Rust's LLVM have incompatible ASan runtimes — see [ASan Runtime Mismatch](#asan-runtime-mismatch-macos) below
- LeakSanitizer is not supported on macOS
- Doctests excluded for the same reason as Linux

### AddressSanitizer — Windows

- **Platform**: Windows (x86_64, MSVC toolchain)
- **Scope**: Full workspace — lib tests, integration tests, binary tests
- **What it catches**: Buffer overflows, use-after-free, double-free in Rust code and at the FFI boundary

Configuration:
- `RUSTFLAGS="-Zsanitizer=address"` — instruments Rust code only
- Rust on Windows MSVC uses **Microsoft's ASan runtime** (from Visual Studio), not LLVM's compiler-rt. The compiler passes `/INFERASANLIBS` to the MSVC linker, which auto-discovers the runtime import library at **link time**. See [PR #118521](https://github.com/rust-lang/rust/pull/118521).
- **Important**: the MSVC ASan runtime DLL (`clang_rt.asan_dynamic-x86_64.dll`) is NOT on PATH by default. The linker finds the import library at build time via `/INFERASANLIBS`, but the DLL loader needs the DLL on PATH at **test runtime**. The CI job uses `vswhere.exe` to locate the DLL directory (e.g., `C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Tools\MSVC\<ver>\bin\Hostx64\x64\`) and prepends it to PATH.
- LLVM is installed only for `libclang` (required by bindgen), pinned to the same version as the regular Windows build job. It is **not** used for the ASan runtime.
- The ReadStat C library is **not** instrumented on Windows currently. Unlike macOS, there is no runtime mismatch — both Rust and `cl.exe` use the same MSVC ASan runtime. Full C instrumentation is a future improvement (see [Future Work](#future-work-windows-c-instrumentation)).
- LeakSanitizer is not supported on Windows
- Doctests excluded for the same reason as Linux

## How `READSTAT_SANITIZE_ADDRESS` Works

The `readstat-sys/build.rs` build script checks for the `READSTAT_SANITIZE_ADDRESS` environment variable. When set, it adds sanitizer flags to the C compiler flags for the ReadStat library only. This is intentionally scoped — a global `CFLAGS` would instrument third-party sys crates (e.g., `zstd-sys`) causing linker failures.

The flags are platform-specific:
- **Linux/macOS**: `-fsanitize=address -fno-omit-frame-pointer` (GCC/Clang syntax)
- **Windows MSVC**: `/fsanitize=address` (MSVC syntax)

Currently only the Linux CI job sets `READSTAT_SANITIZE_ADDRESS=1` because it is the only platform where C instrumentation has been validated.

## ASan Runtime Mismatch (macOS)

**macOS** has an ASan runtime mismatch that prevents instrumenting the C code alongside Rust. Apple Clang is a fork of LLVM with its own ASan runtime versioning. When both Rust and the C library are instrumented, the linker sees two incompatible ASan runtimes and fails with `___asan_version_mismatch_check_apple_clang_*` vs `___asan_version_mismatch_check_v8`. A potential workaround is to install upstream LLVM via Homebrew (`brew install llvm`) and set `CC=/opt/homebrew/opt/llvm/bin/clang` so both the C code and Rust use the same LLVM ASan runtime. However, this is fragile — the Homebrew LLVM version must stay close to the LLVM version used by Rust nightly, which changes frequently.

**Windows does NOT have this problem.** Rust on `x86_64-pc-windows-msvc` uses Microsoft's ASan runtime ([PR #118521](https://github.com/rust-lang/rust/pull/118521)), and so does `cl.exe /fsanitize=address`. Both link the same `clang_rt.asan_dynamic-x86_64.dll` from Visual Studio. Full C + Rust ASan instrumentation is theoretically possible on Windows — see [Future Work](#future-work-windows-c-instrumentation).

**Bottom line**: Linux has full C + Rust ASan coverage. macOS provides Rust-only coverage due to the Apple Clang runtime mismatch. Windows provides Rust-only coverage currently, but full coverage is a future improvement since there is no runtime mismatch.

## Future Work: Windows C Instrumentation

Since Rust and MSVC share the same ASan runtime on Windows, enabling `READSTAT_SANITIZE_ADDRESS=1` in the Windows CI job should allow full C + Rust instrumentation — matching Linux's coverage. This requires:

1. Setting `READSTAT_SANITIZE_ADDRESS=1` so `readstat-sys/build.rs` adds `/fsanitize=address` when compiling the ReadStat C library
2. Verifying there are no linker conflicts (if conflicts arise, the unstable `-Zexternal-clangrt` flag can tell Rust to skip linking its own runtime copy)
3. Ensuring the MSVC ASan runtime DLL is on PATH at test time (the CI job already does this via `vswhere.exe`)

## Running Locally

### Miri
```bash
rustup +nightly component add miri
MIRIFLAGS="-Zmiri-disable-isolation" cargo +nightly miri test -p readstat
```

### ASan on Linux
```bash
RUSTFLAGS="-Zsanitizer=address -Clinker=clang" \
READSTAT_SANITIZE_ADDRESS=1 \
cargo +nightly test --workspace --lib --tests --bins --target x86_64-unknown-linux-gnu
```

### ASan on macOS
```bash
RUSTFLAGS="-Zsanitizer=address" \
cargo +nightly test --workspace --lib --tests --bins --target aarch64-apple-darwin
```

### ASan on Windows
```powershell
$env:RUSTFLAGS = "-Zsanitizer=address"
# The MSVC ASAN runtime DLL must be on PATH. Find it via vswhere:
$vsPath = & "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe" -latest -property installationPath
$msvcVer = (Get-ChildItem "$vsPath\VC\Tools\MSVC" | Sort-Object Name -Descending | Select-Object -First 1).Name
$env:PATH = "$vsPath\VC\Tools\MSVC\$msvcVer\bin\Hostx64\x64;$env:PATH"
cargo +nightly test --workspace --lib --tests --bins --target x86_64-pc-windows-msvc
```

### Valgrind (Linux)

For manual checks with full C library coverage, [valgrind](https://valgrind.org/) can also be used against debug test binaries:

```bash
cargo test -p readstat-tests --no-run
valgrind ./target/debug/deps/parse_file_metadata_test-<hash>
```

## Coverage Summary

| Tool | Platform | Rust code | C code (ReadStat) | Leak detection |
|------|----------|-----------|--------------------|----------------|
| Miri | Linux | Unit tests only | No (FFI excluded) | No |
| ASan | Linux | Full workspace | Yes (instrumented) | Yes |
| ASan | macOS | Full workspace | No (runtime mismatch) | No |
| ASan | Windows | Full workspace | Not yet (no mismatch — see [future work](#future-work-windows-c-instrumentation)) | No |
| Valgrind | Linux (manual) | Full | Full | Yes |
