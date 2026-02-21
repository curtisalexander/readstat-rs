[< Back to README](../README.md)

# Memory Safety

This project contains unsafe Rust code (FFI callbacks, pointer casts, memory-mapped I/O) and links against the vendored ReadStat C library. Three automated CI checks guard against memory errors.

## CI Jobs

All three jobs run on every workflow dispatch and tag push, in parallel with the build jobs. Any memory error fails the job with a nonzero exit code.

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
- The ReadStat C library is **not** instrumented on macOS because Apple Clang and Rust's LLVM have incompatible ASan runtimes (`___asan_version_mismatch_check_apple_clang_*` vs `___asan_version_mismatch_check_v8`)
- LeakSanitizer is not supported on macOS
- Doctests excluded for the same reason as Linux

## How `READSTAT_SANITIZE_ADDRESS` Works

The `readstat-sys/build.rs` build script checks for the `READSTAT_SANITIZE_ADDRESS` environment variable. When set, it adds `-fsanitize=address` and `-fno-omit-frame-pointer` to the C compiler flags for the ReadStat library only. This is intentionally scoped — a global `CFLAGS` would instrument third-party sys crates (e.g., `zstd-sys`) causing linker failures.

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
| Valgrind | Linux (manual) | Full | Full | Yes |
