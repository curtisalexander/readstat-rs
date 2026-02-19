[< Back to README](../README.md)

# Building from Source

## Clone
Ensure submodules are also cloned.

```sh
git clone --recurse-submodules https://github.com/curtisalexander/readstat-rs.git
```

The [ReadStat](https://github.com/WizardMac/ReadStat) repository is included as a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules) within this repository.  In order to build and link, first a [readstat-sys](https://github.com/curtisalexander/readstat-rs/tree/main/readstat-sys) crate is created.  Then the [readstat](https://github.com/curtisalexander/readstat-rs/tree/main/readstat) binary utilizes `readstat-sys` as a dependency.

## Linux
Install developer tools

```sh
# unixodbc-dev needed for full compilation of arrow2
sudo apt install build-essential clang unixodbc-dev
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
Building on Windows requires [LLVM](https://releases.llvm.org/download.html) and [Visual Studio C++ Build tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) be downloaded and installed.

In addition, the path to `libclang` needs to be set in the environment variable `LIBCLANG_PATH`.  If `LIBCLANG_PATH` is not set, the [readstat-sys build script](../crates/readstat-sys/build.rs) will check the default path `C:\Program Files\LLVM\lib` and fail with instructions if it does not exist.

For details see the following.
- [Check for `LIBCLANG_PATH`](https://github.com/curtisalexander/readstat-rs/blob/main/crates/readstat-sys/build.rs#L78-L82)
- [Building in GitHub Actions](https://github.com/curtisalexander/readstat-rs/blob/main/.github/workflows/main.yml#L140-L156)

Build
```sh
cargo build
```

**iconv**: Compiled from source using the vendored [libiconv-win-build](https://github.com/kiyolee/libiconv-win-build) submodule (located at `crates/iconv-sys/vendor/libiconv-win-build/`) via the [iconv-sys](../crates/iconv-sys/) crate. `iconv-sys` is a Windows-only dependency (gated behind `[target.'cfg(windows)'.dependencies]` in [readstat-sys/Cargo.toml](../crates/readstat-sys/Cargo.toml)).

**zlib**: Compiled from source via the [libz-sys](https://crates.io/crates/libz-sys) crate (statically linked).

## Linking Summary

| Platform | iconv | zlib |
|----------|-------|------|
| Linux (glibc/musl) | Dynamic (system) | libz-sys (prefers system, falls back to source) |
| macOS (x86/ARM) | Dynamic (system) | libz-sys (uses system) |
| Windows (MSVC) | Static (vendored submodule) | libz-sys (compiled from source, static) |
