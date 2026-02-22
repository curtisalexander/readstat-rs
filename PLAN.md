# Pre-crates.io Release Cleanup Plan

## Current State Assessment

| Check | Status |
|-------|--------|
| Build | Clean — compiles with zero errors |
| Tests | All pass (1 ignored test in readstat-tests) |
| Clippy (default) | Clean — zero warnings |
| Clippy (pedantic) | ~230 warnings in non-sys crates |
| Formatting | Drift in benchmarks and several source files |
| Doc build | Clean (1 known cargo filename collision warning between `readstat` lib and `readstat` bin) |
| Crate metadata | All 4 publishable crates have complete crates.io metadata |
| TODO/FIXME | None found in any source files |
| `unwrap()` in non-test code | Only in `LazyLock::new()` regex compilation (acceptable) and 1 `CString::new("").unwrap()` in dummy paths |

---

## Phase 1: Code Formatting

Run `cargo fmt --all`. Drift detected in:
- `crates/readstat/benches/readstat_benchmarks.rs` — import order, line wrapping
- `crates/readstat/src/lib.rs` — re-export ordering
- `crates/readstat/src/rs_buffer_io.rs` — function signature line breaks
- `crates/readstat/src/rs_data.rs` — import grouping
- `crates/readstat/src/formats.rs` — array literal formatting

---

## Phase 2: Critical Dependency Fix for crates.io

**`readstat-cli/Cargo.toml`** — The `readstat` dependency is missing a `version` field:
```toml
# Current:
readstat = { path = "../readstat" }
# Required for crates.io:
readstat = { path = "../readstat", version = "0.18.0" }
```
Without this, `cargo publish` will fail for `readstat-cli`.

---

## Phase 3: Remove Unnecessary `extern crate`

**`readstat-cli/src/main.rs:1`** — `extern crate readstat_sys;` is unnecessary in edition 2024. Remove it.

---

## Phase 4: Targeted Clippy Pedantic Fixes

Focus on warnings that represent genuine code quality improvements. Skip warnings from bindgen-generated code and FFI-boundary casts where the C API dictates types.

### 4a. `cb.rs` (~56 warnings)
- Replace `format!("foo is {}", var)` with `format!("foo is {var}")` in ~20 `debug!()` calls
- Replace safe widening `as` casts with `From` (e.g., `v as i16` → `i16::from(v)` for Int8→Int16)
- Add `#[allow(clippy::cast_possible_truncation)]` with comments on FFI-dictated casts (SAS dates `as i32`, timestamps `as i64`)
- Use `if let` instead of `match` for single-pattern destructuring where applicable

### 4b. `rs_write.rs` (~34 warnings)
- Change `&PathBuf` → `&Path` in function signatures (`open_output`, `write_batch_to_parquet`, `merge_parquet_files`)
- Remove double blank line at line 543-544
- Simplify unnecessary `Ok()` wrapping where functions trivially succeed

### 4c. `rs_metadata.rs` (~29 warnings)
- Inline variables in `format!`/`debug!` strings
- Use `if let` for single-pattern matches
- Consider adding a `#[allow(clippy::too_many_lines)]` with a comment for the schema-building function, or extract a helper

### 4d. `rs_data.rs` (~29 warnings)
- Replace safe widening `as` casts with `From`/`TryFrom`
- Inline variables in format strings
- Add `#[allow]` with comment for the `init` function length (148 lines — initialization logic that's clearer as one block)

### 4e. `rs_parser.rs` (~18 warnings)
- Inline variables in `debug!()` calls
- Add `#[allow]` with comments for FFI error code casts (`as i32`)

### 4f. `rs_buffer_io.rs` (~16 warnings)
- Add `#[allow]` with safety comments for raw pointer casts and `usize as isize` at FFI boundary

### 4g. `rs_write_config.rs` (~9 warnings)
- `validate_format` always returns `Ok` — simplify return type or add `#[allow]` if the `Result` is intentional for API consistency
- Inline format string variables

### 4h. `err.rs` (~5 warnings)
- Add backticks around `READSTAT_OK` in doc comments

### 4i. `rs_path.rs` (~4 warnings)
- Minor doc/format cleanups

### 4j. `readstat-cli/run.rs` (~10 warnings)
- Replace safe `as` casts with `From` where applicable
- Add `#[allow(clippy::too_many_lines)]` to `run()` with a comment explaining the linear dispatch structure

### 4k. `readstat-cli/main.rs` (1 warning)
- Covered by Phase 3 (remove `extern crate`)

---

## Phase 5: Documentation Refinements

1. **CHANGELOG.md** — Set release date for 0.18.0
2. **README.md** — Add crates.io and docs.rs badges for the published crates
3. **Verify doc build** — Confirm `cargo doc --workspace --no-deps` has no new warnings
4. **Review `#![warn(missing_docs)]`** — Already enabled on `readstat` crate; confirm all public items are documented

---

## Phase 6: Final Verification

1. `cargo fmt --all -- --check` — zero diff
2. `cargo clippy --workspace` — zero warnings
3. `cargo test --workspace` — all pass
4. `cargo doc --workspace --no-deps` — clean
5. `cargo package --list -p readstat-sys` — verify contents
6. `cargo package --list -p iconv-sys` — verify contents
7. `cargo package --list -p readstat` — verify contents
8. `cargo package --list -p readstat-cli` — verify contents

---

## Questions for Owner

1. **Pedantic clippy scope**: Should I fix all ~230 actionable pedantic warnings, or focus only on the high-impact ones (function signatures with `&PathBuf` → `&Path`, `From` instead of `as` for safe widenings, format string modernization)? The remaining warnings are mostly cast warnings in FFI callbacks where the C API dictates the types.

2. **`run.rs` refactoring**: The 439-line `run()` function handles 3 subcommands × multiple modes. Should I extract helper functions for the Data subcommand's parallel-write and SQL branches, or leave it as-is since it's a CLI dispatch function with clear section comments?

3. **CHANGELOG release date**: Should I set 0.18.0's date to today (2026-02-22)?

4. **Publish order**: crates.io requires dependencies to be published first. The order would be: `iconv-sys` → `readstat-sys` → `readstat` → `readstat-cli`. Should both `readstat` and `readstat-cli` use `0.18.0`? (They currently do.)

5. **`readstat-tests` cleanup**: This is internal-only. Should I apply the same cleanup standards, or focus exclusively on the 4 publishable crates?
