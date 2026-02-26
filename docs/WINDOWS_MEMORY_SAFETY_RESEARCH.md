[< Back to MEMORY_SAFETY](MEMORY_SAFETY.md)

# Windows Memory Safety: Research & Options

Research into the feasibility of AddressSanitizer (ASAN) on Windows for Rust
projects that link C libraries through FFI, and alternative approaches.

## Table of Contents

- [Executive Summary](#executive-summary)
- [Current State of Rust ASAN on Windows](#current-state-of-rust-asan-on-windows)
- [Critical Finding: Runtime Architecture](#critical-finding-runtime-architecture)
- [The CI Problem](#the-ci-problem)
- [Option 1: Fix ASAN (Rust-only instrumentation)](#option-1-fix-asan-rust-only-instrumentation)
- [Option 2: Fix ASAN (Full Rust + C instrumentation)](#option-2-fix-asan-full-rust--c-instrumentation)
- [Option 3: cargo-careful](#option-3-cargo-careful)
- [Option 4: Windows Page Heap (GFlags)](#option-4-windows-page-heap-gflags)
- [Option 5: Dr. Memory](#option-5-dr-memory)
- [Option 6: Drop the Windows ASAN job entirely](#option-6-drop-the-windows-asan-job-entirely)
- [Comparison Matrix](#comparison-matrix)
- [Recommendation](#recommendation)
- [References](#references)

---

## Executive Summary

ASAN on Windows for Rust is **functional but immature**, and our CI setup has a
fundamental misunderstanding of how it works. The key finding: Rust on
`x86_64-pc-windows-msvc` uses **Microsoft's ASAN runtime** (from Visual Studio),
NOT the LLVM compiler-rt runtime. Our CI installs standalone LLVM and puts its
ASAN DLL on the PATH, which is either unnecessary or actively harmful.

There are several viable paths forward, each with different trade-offs between
coverage depth, CI complexity, and reliability.

---

## Current State of Rust ASAN on Windows

### Official Support

- **Merged**: [PR #118521](https://github.com/rust-lang/rust/pull/118521) by
  Daniel Paoliello (late 2023) added ASAN support for `x86_64-pc-windows-msvc`
- **Closed issue**: [#89339](https://github.com/rust-lang/rust/issues/89339)
  (Windows LLVM Sanitizer Support)
- **Status**: Unstable, requires nightly (`-Zsanitizer=address`)
- **Stabilization in progress**:
  [#123615](https://github.com/rust-lang/rust/issues/123615) /
  [PR #123617](https://github.com/rust-lang/rust/pull/123617)

### Known Limitations

1. **Nightly only** — `-Zsanitizer=address` is an unstable flag
2. **No LeakSanitizer** — Windows lacks the `ptrace`-based `StopTheWorld`
   mechanism needed for leak scanning
3. **LLD linker incompatibility** — [#138222](https://github.com/rust-lang/rust/issues/138222):
   `asan_odr_windows.rs` test fails with `lld` because it cannot find
   `librustc-dev_rt.asan.a`. The MSVC linker works.
4. **No cross-compilation** — ASAN requires the MSVC runtime DLLs, which are
   found via `cl.exe` directory traversal at build time
5. **Requires Visual Studio** — the ASAN runtime is shipped as part of the
   Visual Studio C++ workload

### Community Experience

- Very few people have used Rust ASAN on Windows in practice
- The [geo-ant blog post](https://geo-ant.github.io/blog/2024/rust-address-sanitizer-with-c/)
  on ASAN with Rust+C FFI is Linux-focused; the author noted being "not sure
  what the status of AddressSanitizer is on Windows"
- The [rustls-ffi ASAN crash](https://github.com/abetterinternet/crustls/issues/80)
  is one of the few real-world examples of ASAN + FFI on Windows, and it
  involved ASAN runtime conflicts
- The [KDAB blog post](https://www.kdab.com/cpp-projects-asan-windows/) on
  building C++ with ASAN on Windows recommends using clang over MSVC's `cl.exe`
  due to incomplete MSVC support

---

## Critical Finding: Runtime Architecture

This is the most important thing we got wrong in our current setup.

### How Rust ASAN Works on Each Platform

| Platform | Rust ASAN runtime | C compiler for ReadStat | C ASAN compatible? |
|----------|-------------------|------------------------|-------------------|
| Linux | LLVM compiler-rt | `cc` (system GCC/Clang) | Yes — same LLVM runtime via `-Clinker=clang` |
| macOS | LLVM compiler-rt | Apple Clang | No — Apple Clang has its own ASan runtime fork |
| **Windows** | **MSVC's ASAN runtime** | **MSVC `cl.exe`** | **Yes — same MSVC runtime via `/fsanitize=address`** |

### The Key Insight

[PR #118521](https://github.com/rust-lang/rust/pull/118521) specifically chose
to use **Microsoft's ASAN runtime** (the `clang_rt.asan_dynamic-*.dll` shipped
with Visual Studio), not LLVM's compiler-rt. From Daniel Paoliello's
[announcement](https://hachyderm.io/@TehPenguin/111700487240594293):

> "It uses the MSVC ASAN libraries, so you can safely link with C/C++ code
> built by MSVC with its ASAN enabled."

This means:

1. **Rust and MSVC C code use the SAME ASAN runtime on Windows** — there is no
   runtime mismatch like on macOS
2. Our `MEMORY_SAFETY.md` documentation claiming a "runtime mismatch" on
   Windows is **incorrect**
3. **Full C + Rust ASAN instrumentation should be possible** on Windows, just
   like on Linux
4. The standalone LLVM installation in CI may be providing the **wrong** ASAN
   DLL (LLVM's instead of MSVC's)

### Where the MSVC ASAN DLL Lives

The GitHub Actions `windows-latest` runner has Visual Studio pre-installed. The
ASAN runtime DLL is at a path like:

```
C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Tools\MSVC\<version>\bin\Hostx64\x64\clang_rt.asan_dynamic-x86_64.dll
```

Rust's nightly toolchain finds this via `cl.exe` directory traversal — it does
NOT need us to install a separate LLVM for the ASAN runtime.

---

## The CI Problem

Our current `asan-windows` job (7 iterative fix commits) has this architecture:

1. Install standalone LLVM (matching Rust nightly's LLVM version)
2. Set `LIBCLANG_PATH` to the standalone LLVM (for bindgen)
3. Add standalone LLVM's ASAN DLL directory to `PATH`
4. Run tests

**Problems:**
- Step 3 may put the wrong ASAN DLL on PATH (LLVM's vs. MSVC's)
- The LLVM version must exactly match Rust nightly's LLVM, and nightly updates
  frequently, leading to version chasing
- The `force-url` fallback downloads from LLVM releases, which may not have
  matching Windows binaries for every version
- The LLVM installation is large and slow to download/cache

**What's actually needed:**
- `LIBCLANG_PATH` for bindgen (this IS needed for the build)
- The MSVC ASAN runtime on PATH (this is ALREADY available from Visual Studio)

---

## Option 1: Fix ASAN (Rust-only instrumentation)

**What**: Simplify the CI job to just use Rust's ASAN with the MSVC runtime
already present on the runner. Only Rust code is instrumented. The C library
(ReadStat) is NOT instrumented.

**Changes needed:**
- Remove the version-detection and ASAN DLL PATH manipulation
- Keep LLVM install only for `LIBCLANG_PATH` (bindgen needs it)
- Remove `READSTAT_SANITIZE_ADDRESS` from env (don't instrument C code)
- Ensure the MSVC ASAN runtime DLL is on PATH (it may already be via
  `VsDevCmd.bat` or similar)

**Simplified CI job:**
```yaml
asan-windows:
  runs-on: windows-latest
  env:
    RUSTFLAGS: "-Zsanitizer=address"
  steps:
    - uses: actions/checkout@v4
      with:
        submodules: recursive
    - uses: dtolnay/rust-toolchain@nightly
      with:
        targets: x86_64-pc-windows-msvc
    - uses: Swatinem/rust-cache@v2
    # LLVM for bindgen only (same as build-win job)
    - uses: actions/cache@v4
      id: cache-llvm
      with:
        path: ${{ runner.temp }}\llvm
        key: llvm-21.1.8
    - uses: KyleMayes/install-llvm-action@v2
      with:
        version: "21.1.8"
        directory: ${{ runner.temp }}\llvm
        cached: ${{ steps.cache-llvm.outputs.cache-hit }}
    - name: Run tests with AddressSanitizer
      run: |
        $env:LIBCLANG_PATH = "${{ runner.temp }}\llvm\bin"
        cargo +nightly test --workspace --lib --tests --bins --target x86_64-pc-windows-msvc
```

**Pros:**
- Much simpler — no version chasing, no DLL path hacking
- LLVM version is pinned (only needs libclang, not ASAN runtime)
- Catches Rust-side memory errors and FFI boundary issues
- Same level of coverage as current macOS ASAN job

**Cons:**
- Does not instrument the ReadStat C library
- Misses bugs that originate purely in C code (buffer overflows in C, etc.)

---

## Option 2: Fix ASAN (Full Rust + C instrumentation)

**What**: Instrument BOTH Rust and C code with ASAN, using the MSVC ASAN runtime
for both. This is the "ideal" setup and IS theoretically possible because Rust
and MSVC's `cl.exe` share the same ASAN runtime on Windows.

**Changes needed:**
- Set `READSTAT_SANITIZE_ADDRESS=1` to instrument C code with `/fsanitize=address`
- Ensure the `cc` crate uses MSVC's `cl.exe` (it should by default on MSVC target)
- Ensure the MSVC ASAN runtime DLL is on PATH at test time
- May need the `-Zexternal-clangrt` flag if there are linker conflicts between
  Rust's copy of the runtime and the one linked by the C code

**Key risk:** While PR #118521 says Rust uses the MSVC ASAN libraries,
the actual linking behavior may be more nuanced. The `cc` crate compiles C code
with `cl.exe /fsanitize=address`, which links `clang_rt.asan*.lib` from MSVC.
Rust also links its own ASAN runtime. If they're truly the same library (same
DLL), this should work. If they're different builds of the same library, linker
conflicts may arise.

**If linker conflicts occur**, the `-Zexternal-clangrt` flag
([PR #121207](https://github.com/rust-lang/rust/pull/121207)) can tell Rust to
skip linking its own copy and use the one from the C code instead.

**Pros:**
- Full coverage of both Rust and C code — best possible detection
- Would make Windows ASAN comparable to Linux ASAN
- Theoretically sound since both use MSVC's runtime

**Cons:**
- Untested — no one has publicly documented this working
- May hit edge cases in the nightly compiler
- `-Zexternal-clangrt` is itself an unstable flag, compounding instability
- Higher risk of CI breakage from nightly Rust changes

---

## Option 3: cargo-careful

**What**: Use [cargo-careful](https://github.com/RalfJung/cargo-careful) to run
tests with extra debug assertions enabled in the standard library.

**How it works:**
- Rebuilds the standard library from source with debug assertions enabled
- Catches UB in Rust code: invalid `char::from_u32_unchecked`, null/unaligned
  pointer operations, overlapping `copy`/`copy_nonoverlapping`, etc.
- Works on ALL platforms including Windows
- Fully FFI-compatible (C code runs uninstrumented)

**CI job:**
```yaml
careful-windows:
  runs-on: windows-latest
  steps:
    - uses: actions/checkout@v4
      with:
        submodules: recursive
    - uses: dtolnay/rust-toolchain@nightly
      with:
        targets: x86_64-pc-windows-msvc
        components: rust-src
    - uses: Swatinem/rust-cache@v2
    - uses: actions/cache@v4
      id: cache-llvm
      with:
        path: ${{ runner.temp }}\llvm
        key: llvm-21.1.8
    - uses: KyleMayes/install-llvm-action@v2
      with:
        version: "21.1.8"
        directory: ${{ runner.temp }}\llvm
        cached: ${{ steps.cache-llvm.outputs.cache-hit }}
    - run: cargo install cargo-careful
    - name: Run tests carefully
      run: |
        $env:LIBCLANG_PATH = "${{ runner.temp }}\llvm\bin"
        cargo +nightly careful test --workspace --target x86_64-pc-windows-msvc
```

**Pros:**
- Stable, well-maintained tool (by Ralf Jung, Miri's creator)
- Works reliably on Windows — no DLL path issues
- No ASAN runtime to manage — just a rebuilt stdlib
- FFI-compatible — C code executes normally
- Catches real bugs in `unsafe` Rust code
- Much simpler CI than ASAN

**Cons:**
- Does NOT detect C-side memory errors (buffer overflows, use-after-free in
  ReadStat)
- Less comprehensive than ASAN — misses out-of-bounds pointer arithmetic,
  use-after-free, etc.
- Requires `rust-src` component and stdlib rebuild (slower first build)
- Does not detect leaks

---

## Option 4: Windows Page Heap (GFlags)

**What**: Use Windows' built-in Page Heap facility to detect heap memory errors
at the OS level. Works on ANY native binary, including Rust + C FFI.

**How it works:**
- `gflags.exe /p /enable <exe> /full` enables full page heap for a binary
- Places an inaccessible guard page after (or before) every heap allocation
- Any out-of-bounds access immediately triggers an access violation
- Works across language boundaries — C and Rust allocations both protected

**CI approach:**
```yaml
pageheap-windows:
  runs-on: windows-latest
  steps:
    # ... build steps ...
    - name: Enable Page Heap
      run: |
        # Build test binaries without running them
        cargo test --workspace --no-run --target x86_64-pc-windows-msvc 2>&1 |
          Select-String 'Executable' | ForEach-Object {
            $exe = ($_ -split '\(')[1].TrimEnd(')')
            gflags.exe /p /enable (Split-Path $exe -Leaf) /full
          }
    - name: Run tests with Page Heap
      run: cargo test --workspace --target x86_64-pc-windows-msvc
```

**Pros:**
- Works on BOTH Rust and C code — no runtime mismatch issues
- No compiler instrumentation — works on any binary
- Detects heap buffer overflows, use-after-free, double-free
- Built into Windows — no extra tools to install
- Catches errors in the ReadStat C library

**Cons:**
- Very high memory usage (one guard page per allocation = ~4KB overhead per
  `malloc`)
- Can trigger false OOM on memory-intensive tests
- Only detects heap errors (not stack overflows or global buffer overflows)
- May slow tests significantly
- Error reporting is via access violations (crash), not structured ASAN-style
  reports with stack traces
- Requires WinDbg or similar for readable stack traces
- Not commonly used in CI — unusual setup

---

## Option 5: Dr. Memory

**What**: [Dr. Memory](https://drmemory.org/) is a memory error detection tool
(similar to Valgrind) that works on Windows via dynamic binary instrumentation.

**How it works:**
- Uses DynamoRIO to instrument the binary at runtime
- Detects uninitialized reads, out-of-bounds access, use-after-free, double-free,
  memory leaks, handle leaks (Windows-specific)
- Works on unmodified binaries — no compiler flags needed

**Pros:**
- Works on BOTH Rust and C code — full coverage like Valgrind on Linux
- No compiler instrumentation or runtime library management
- Detects a wide range of errors including leaks
- Cross-language — instruments everything in the process
- Claims 2x faster than Valgrind's Memcheck

**Cons:**
- Last release (2.3.x) is aging — uncertain maintenance status
- x86/x64 only (not ARM)
- Significant runtime overhead (typical of dynamic instrumentation)
- Not widely used in the Rust community — untested territory
- May produce false positives on Rust runtime internals
- CI integration is non-trivial
- May not handle Rust's allocation patterns well

---

## Option 6: Drop the Windows ASAN job entirely

**What**: Remove the `asan-windows` CI job and rely on Linux ASAN (which has full
C+Rust coverage) plus Miri for memory safety.

**Rationale:**
- Linux ASAN already instruments both Rust and C code
- Memory bugs in the ReadStat C library are platform-independent (the C code is
  identical across platforms)
- Miri catches Rust-specific UB
- The Windows ASAN job has been a maintenance burden (7 fix commits) with
  diminishing returns
- Windows-specific memory bugs are extremely unlikely given that the same code
  runs on Linux with full ASAN instrumentation

**Pros:**
- Zero maintenance overhead
- No risk of CI flakiness from nightly Rust + ASAN version chasing
- Linux ASAN already provides the coverage that matters most

**Cons:**
- Loses any Windows-specific memory checking
- Theoretically, Windows-specific code paths (iconv-sys, Windows file I/O)
  go unchecked
- Perception issue — looks like less comprehensive CI

---

## Comparison Matrix

| | ASAN Rust-only | ASAN Full | cargo-careful | Page Heap | Dr. Memory | No job |
|---|---|---|---|---|---|---|
| **Rust code coverage** | Yes | Yes | Partial (stdlib assertions) | Heap only | Yes | No (on Windows) |
| **C code coverage** | No | Yes | No | Heap only | Yes | No (on Windows) |
| **FFI boundary** | Yes | Yes | Partial | Yes | Yes | No (on Windows) |
| **Leak detection** | No | No | No | No | Yes | No |
| **CI complexity** | Medium | High | Low | Medium | Medium | None |
| **CI reliability** | Medium | Low (untested) | High | Medium | Low (untested) | N/A |
| **Runtime overhead** | ~2x | ~2x | ~1.5x | ~10x+ | ~5-10x | N/A |
| **Maintenance burden** | Medium | High | Low | Low | Medium | None |
| **Community precedent** | Some | None | Established | Rare in CI | Rare for Rust | Common |

---

## Recommendation

Based on this research, here is a recommended tiered approach:

### Tier 1 (Do now): Simplify ASAN to Rust-only (Option 1)

Fix the current CI job by removing the LLVM version chasing and ASAN DLL path
manipulation. Just use Rust's ASAN with the MSVC runtime that's already on the
GitHub Actions runner. This eliminates the maintenance burden while still
catching Rust-side memory errors and FFI boundary issues.

The LLVM install is still needed for `LIBCLANG_PATH` (bindgen), but we can pin
it to a specific version like the `build-win` job does — no need to match Rust
nightly's LLVM version.

### Tier 2 (Try next): Attempt full Rust + C ASAN (Option 2)

Since Rust and MSVC share the same ASAN runtime on Windows, full instrumentation
SHOULD work. Try enabling `READSTAT_SANITIZE_ADDRESS=1` with the simplified
setup. If linker conflicts arise, try `-Zexternal-clangrt`. This would give
Windows the same coverage as Linux.

### Tier 3 (Complement): Add cargo-careful (Option 3)

Whether or not ASAN works, `cargo-careful` is a lightweight, reliable
complement. It catches different classes of bugs (stdlib contract violations)
and could run as a separate job or replace ASAN if ASAN proves too fragile.

### What NOT to pursue

- **Page Heap / GFlags**: Too much memory overhead for CI, poor error reporting
- **Dr. Memory**: Aging tool, untested with Rust, high risk of false positives
- **Dropping the job entirely**: Linux ASAN covers the C code, but we'd lose
  Windows-specific coverage of the Rust code and iconv-sys integration

---

## References

### Rust Sanitizer Issues & PRs
- [#39699 — Tracking issue for sanitizer support](https://github.com/rust-lang/rust/issues/39699)
- [#89339 — Windows LLVM Sanitizer Support](https://github.com/rust-lang/rust/issues/89339) (closed by PR #118521)
- [#118521 — Add ASAN support for Windows MSVC](https://github.com/rust-lang/rust/pull/118521)
- [#121207 — Add -Zexternal-clangrt](https://github.com/rust-lang/rust/pull/121207)
- [#123615 — Tracking issue for stabilizing sanitizers](https://github.com/rust-lang/rust/issues/123615)
- [#123617 — Stabilize AddressSanitizer and LeakSanitizer](https://github.com/rust-lang/rust/pull/123617)
- [#138222 — asan_odr_windows.rs fails with lld](https://github.com/rust-lang/rust/issues/138222)
- [#943 — Allow using prebuilt sanitizer libraries](https://github.com/rust-lang/compiler-team/issues/943)

### Documentation
- [Rust Unstable Book: sanitizer](https://doc.rust-lang.org/beta/unstable-book/compiler-flags/sanitizer.html)
- [Rust Unstable Book: external-clangrt](https://doc.rust-lang.org/nightly/unstable-book/compiler-flags/external-clangrt.html)
- [Rustc Dev Guide: Sanitizers](https://rustc-dev-guide.rust-lang.org/sanitizers.html)
- [MSVC AddressSanitizer](https://learn.microsoft.com/en-us/cpp/sanitizers/asan?view=msvc-170)
- [MSVC ASAN Building Reference](https://learn.microsoft.com/en-us/cpp/sanitizers/asan-building?view=msvc-170)
- [MSVC ASAN Runtime](https://learn.microsoft.com/en-us/cpp/sanitizers/asan-runtime?view=msvc-170)
- [MSVC ASAN One DLL Blog Post](https://devblogs.microsoft.com/cppblog/msvc-address-sanitizer-one-dll-for-all-runtime-configurations/)

### Community & Blog Posts
- [Daniel Paoliello: ASAN for Rust on Windows announcement](https://hachyderm.io/@TehPenguin/111700487240594293)
- [geo-ant: Using ASAN for a C Library Linked to a Rust Executable](https://geo-ant.github.io/blog/2024/rust-address-sanitizer-with-c/)
- [KDAB: How to Build C++ Projects with ASAN on Windows](https://www.kdab.com/cpp-projects-asan-windows/)
- [Google Sanitizers Wiki: Windows Port](https://github.com/google/sanitizers/wiki/AddressSanitizerWindowsPort)
- [rustls-ffi ASAN crashes on Windows](https://github.com/abetterinternet/crustls/issues/80)

### Tools
- [cargo-careful](https://github.com/RalfJung/cargo-careful)
- [Dr. Memory](https://drmemory.org/)
- [GFlags and PageHeap (Microsoft)](https://learn.microsoft.com/en-us/windows-hardware/drivers/debugger/gflags-and-pageheap)

### Academic
- [RustSan: Retrofitting AddressSanitizer for Efficient Sanitization of Rust (USENIX Security 2024)](https://www.usenix.org/conference/usenixsecurity24/presentation/cho-kyuwon)
- [ERASan: Efficient Rust Address Sanitizer (IEEE S&P 2024)](https://www.computer.org/csdl/proceedings-article/sp/2024/313000a239/1WPcYZde4BW)
