# readstat-sys

Raw FFI bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library, generated with [bindgen](https://rust-lang.github.io/rust-bindgen/).

The `build.rs` script compiles ~49 C source files from the vendored `vendor/ReadStat/` git submodule via the `cc` crate and generates Rust bindings with `bindgen`. Platform-specific linking for iconv and zlib is handled automatically (see [docs/BUILDING.md](../../docs/BUILDING.md) for details).

This is a [sys crate](https://kornel.ski/rust-sys-crate) — it exposes raw C types and functions. Use the `readstat` library crate for a safe, high-level API.
