# Crates.io Release Preparation Plan

Comprehensive cleanup of the readstat-rs workspace before publishing to crates.io.

---

## Phase 1: Fix All Clippy Warnings (Default Lints)

These are the warnings that show up with a plain `cargo clippy --workspace`.

### 1.1 `readstat-sys/build.rs` — Collapse nested `if` statements
- **Lines 96-100**: Collapse `if !is_emscripten { if let Some(include) = ... }` into `if !is_emscripten && let Some(include) = ...`
- **Lines 103-107**: Same pattern for `DEP_Z_INCLUDE`

### 1.2 `readstat/src/rs_metadata.rs` — Too many arguments
- **Line 398**: `ReadStatVarMetadata::new()` has 8 args (max recommended: 7). Refactor to use a builder or struct argument.

### 1.3 `readstat/src/rs_path.rs` — `&PathBuf` instead of `&Path`
- **Line 45** (Unix variant): Change `path: &PathBuf` → `path: &Path`

### 1.4 `readstat/src/rs_write.rs` — Manual `is_multiple_of`
- **Line 732**: Replace `(len - i) % 3 == 0` with `(len - i).is_multiple_of(3)`

### 1.5 `readstat-cli/src/run.rs` — Collapse nested `if`
- **Lines 602-612**: Collapse `if !all_temp_files.is_empty() { if let Some(out_path) = ... }` into single condition

### 1.6 `iconv-sys/build.rs` — Unneeded unit expression
- **Line 88**: Remove trailing `()`

---

## Phase 2: Replace `lazy_static` with `std::sync::LazyLock`

### 2.1 `readstat/src/formats.rs` — Replace `lazy_static!` with `LazyLock`
- Replace 5 `lazy_static!` Regex declarations with `static REG: LazyLock<Regex> = LazyLock::new(|| ...)`
- Remove `lazy_static` dependency from `readstat/Cargo.toml`

---

## Phase 3: Deduplicate Compression Resolution

### 3.1 Extract shared `resolve_compression` function
- `rs_write.rs:220-258` and `rs_query.rs:279-317` contain identical `resolve_compression` / `resolve_parquet_compression` logic
- Extract to a shared location (e.g. `rs_write_config.rs` or a new `rs_compression.rs` module) and call from both places

---

## Phase 4: Fix Code Quality Issues in `readstat-cli`

### 4.1 Add missing `value_hint` attributes in `cli.rs`
- **Line 40**: Add `value_hint = ValueHint::FilePath` to `Preview::input`
- **Line 75**: Add `value_hint = ValueHint::FilePath` to `Data::output`

### 4.2 Fix `Display` implementations in `cli.rs`
- **Lines 154, 170, 206**: `CliOutFormat`, `Reader`, and `CliParquetCompression` all use `write!(f, "{:?}", &self)` (Debug format) for Display. Implement proper Display matching the lowercase/expected variant names.

### 4.3 Fix error output in `run.rs`
- **Line 471**: Change `println!` → `eprintln!` for error messages (errors should go to stderr)
- **Line 471**: Fix typo "occured" → "occurred"

### 4.4 Replace bare `.unwrap()` with `.expect()` in `run.rs`
- **Line 492**: `sql_query.as_ref().unwrap()` → `.expect("...")`
- **Line 568**: `schema.as_ref().unwrap()` → `.expect("...")`
- **Line 607**: `schema.as_ref().unwrap()` → `.expect("...")`

---

## Phase 5: Improve Safety Documentation

### 5.1 Add `# Safety` sections to FFI callbacks in `cb.rs`
- `handle_metadata()` (line 39)
- `handle_variable()` (line ~121)
- `handle_value()` (line ~231)

### 5.2 Document magic constants in `cb.rs`
- `DAY_SHIFT: i32 = 3653` — SAS epoch (1960-01-01) to Unix epoch (1970-01-01) day offset
- `SEC_SHIFT: i64 = 315619200` — SAS epoch to Unix epoch second offset
- `ROUND_SCALE: f64 = 1e14` — Rounding scale for sub-second precision

---

## Phase 6: Suppress Bindgen-Generated Clippy Warnings

### 6.1 Add clippy suppression for auto-generated code in `readstat-sys/src/lib.rs`
- The 12 warnings about `missing_safety_doc`, `ptr_offset_with_cast` come from auto-generated `bindings.rs`
- Add `#![allow(clippy::missing_safety_doc)]` and `#![allow(clippy::ptr_offset_with_cast)]` to `lib.rs` since we don't control bindgen output

---

## Phase 7: Crate Metadata Polish

### 7.1 Add `rust-version` (MSRV) to all publishable crates
- Determine the actual MSRV (edition 2024 requires Rust 1.85+)
- Add `rust-version = "1.85"` to Cargo.toml for: `readstat`, `readstat-cli`, `readstat-sys`, `iconv-sys`

### 7.2 Add `homepage` to publishable crates
- Add `homepage = "https://github.com/curtisalexander/readstat-rs"` to crates missing it

### 7.3 Add `readme` to crates missing it
- `readstat-sys/Cargo.toml`: Add `readme = "README.md"`
- `iconv-sys/Cargo.toml`: Add `readme = "README.md"`

### 7.4 Ensure `readstat-tests` is not published
- Add `publish = false` to `readstat-tests/Cargo.toml` if not already present

---

## Phase 8: Remove Dead Code / Clean Up Allows

### 8.1 Evaluate `#[allow(dead_code)]` on `ReadStatHandler` enum in `cb.rs`
- If `READSTAT_HANDLER_ABORT` and `READSTAT_HANDLER_SKIP_VARIABLE` are unused, consider removing the enum entirely and using the integer constants directly, or keep it documented as a reference for the C API contract

---

## Phase 9: Minor Idiomatic Improvements

### 9.1 Use `let...else` where appropriate
- Check for `match`/`if let` patterns that could use `let...else` (clippy pedantic flagged 2 instances)

### 9.2 Clean up unnecessary raw string hashes
- 3 instances of `r#"..."#` where the string doesn't contain `"` — simplify to `r"..."`

### 9.3 Use `format!` variable capture
- 25 instances where `format!("{}", var)` could be `format!("{var}")`

---

## Phase 10: Verify & Validate

### 10.1 Run `cargo clippy --workspace` — should produce zero warnings
### 10.2 Run `cargo test --workspace` — all tests should pass
### 10.3 Run `cargo doc --workspace --no-deps` — docs should build cleanly
### 10.4 Run `cargo publish --dry-run` on each publishable crate to verify packaging

---

## Questions for Discussion

1. **MSRV**: Is `1.85` the right minimum? Or do you want to support older Rust versions? (Edition 2024 requires at least 1.85.)

2. **`lazy_static` → `LazyLock`**: `LazyLock` was stabilized in Rust 1.80. Since we're already on edition 2024 (requiring 1.85+), this is safe. Do you want to proceed with this modernization?

3. **`ReadStatVarMetadata::new()` too-many-args**: The clippy lint flags 8 args (limit 7). Options:
   - (a) Suppress with `#[allow(clippy::too_many_arguments)]` — it's a constructor, arguably fine
   - (b) Switch to a builder pattern
   - (c) Group some args into a sub-struct
   Which approach do you prefer?

4. **`ReadStatHandler` enum in `cb.rs`**: It has `#[allow(dead_code)]` because `READSTAT_HANDLER_ABORT` and `READSTAT_HANDLER_SKIP_VARIABLE` are defined but only `READSTAT_HANDLER_OK` is used. Keep it as documentation of the C API contract, or remove?

5. **Pedantic clippy lints**: The pedantic pass shows ~300 warnings (many from bindgen output). Should we enable any specific pedantic lints workspace-wide, or just fix the default-level ones?

6. **`readstat` crate description**: Currently "Rust wrapper of the ReadStat C library" — do you want something more descriptive for crates.io, like "Read SAS binary files (.sas7bdat) and convert to Arrow, Parquet, CSV, and other formats"?
