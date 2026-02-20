# Agent Instructions

## First Steps

Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) to understand the project structure, crate layout, key types, and architectural patterns before making changes.

## Build & Test

```bash
# Build (ensure git submodules are initialized first)
git submodule update --init --recursive
cargo build

# Run all tests
cargo test --workspace

# Run a specific test crate
cargo test -p readstat-tests

# Run a specific test
cargo test -p readstat-tests <test_name>
```

## Conventions

- Rust edition 2024
- Main branch: `main`, development branch: `dev`
- Workspace with 5 crates under `crates/` plus `readstat-wasm` (excluded from workspace, built separately with Emscripten)
- `readstat` is a pure library; `readstat-cli` owns the binary
- FFI crates follow the `*-sys` naming convention with `build.rs` + bindgen
- Integration tests are in the separate `readstat-tests` crate, not inline
- Arrow v57 ecosystem — keep Arrow crate versions in sync
