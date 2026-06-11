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
- Workspace with 5 crates under `crates/` plus `readstat-wasm` (excluded from workspace, built separately with Emscripten) and `fuzz/` (standalone cargo-fuzz project, requires nightly)
- `readstat` is a pure library; `readstat-cli` owns the binary
- FFI crates follow the `*-sys` naming convention with `build.rs` + bindgen
- Integration tests are in the separate `readstat-tests` crate, not inline
- Arrow v58 ecosystem — keep all Arrow crate versions in sync (pinned once in `[workspace.dependencies]`)
- **Arrow/Parquet are locked to DataFusion**: each `datafusion` release requires a specific `arrow` major (e.g. datafusion 54 → arrow ^58). Never bump `arrow`/`parquet` ahead of a `datafusion` release that supports the new major — cargo will silently resolve two arrow majors and the `sql` feature stops compiling. Bump the whole set together, or hold arrow back. `scripts/check-arrow-lockstep.sh` (run in CI and by `check-updates.sh`) fails if the lockfile ever splits.

## Windows Tool Paths

- **GitHub CLI**: `"/c/Program Files/GitHub CLI/gh.exe"` (not on default bash PATH)
