# Contributing orientation

This is a map of the repository, not a separate set of contribution rules.

- `crates/readstat`: public Rust reader/writer and SQL APIs.
- `crates/readstat-cli`: command parsing, conversion orchestration, human metadata output, and columns-file parsing.
- `crates/readstat-sys` / `readstat-iconv-sys`: raw C FFI and vendored builds. Normal builds consume checked-in bindings; regeneration is the explicit maintainer workflow documented in [CI-CD](docs/CI-CD.md#updating-bindgen-or-the-vendored-c--regenerating-bindings).
- `crates/readstat-tests`: integration coverage and datasets.
- `crates/readstat-wasm` and `fuzz`: standalone projects excluded from the workspace.
- `examples`: independently built demos; they are not workspace members.

Start with [Architecture](docs/ARCHITECTURE.md), then the README in the crate you are changing. Core checks are `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `cargo test --workspace --all-features`. Build the book with `bash scripts/build-book.sh` (PowerShell equivalent available).

For release preparation, dependency order, vendoring, and tags, follow [Releasing](docs/RELEASING.md).
