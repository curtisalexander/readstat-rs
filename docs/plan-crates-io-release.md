# Plan: Prepare crates.io Release

## Summary

Thorough codebase cleanup before the first crates.io publish of `iconv-sys`, `readstat-sys`, `readstat`, and `readstat-cli`. Covers naming conflicts, packaging, code quality, clippy, documentation, and publish workflow.

---

## 1. BLOCKING: Rename `iconv-sys` (name conflict on crates.io)

There is already an `iconv-sys` crate on crates.io (v0.0.4) owned by a different author. We **cannot** publish under that name.

**Proposed rename:** `readstat-iconv-sys`

This involves:
- Rename `package.name` in `crates/iconv-sys/Cargo.toml`
- Update the dependency reference in `crates/readstat-sys/Cargo.toml`
- Update `DEP_ICONV_INCLUDE` references in `readstat-sys/build.rs` (the env var name is derived from the `links` key, not the package name, so this should still work since `links = "iconv"` is unchanged)
- Update docs (ARCHITECTURE.md, README.md, CHANGELOG.md)

## 2. BLOCKING: Trim `iconv-sys` package size (25 MB → ~2 MB)

The vendored libiconv submodule includes 1,695 files (25.3 MB) — most are unnecessary Visual Studio project files and test suites. Add an `include` field to `crates/iconv-sys/Cargo.toml` to only ship the files the `build.rs` actually needs (source `.c/.h` files, `lib/`, the license, and the build script itself).

Similarly, review `readstat-sys` (225 files) and add `include` to exclude unnecessary files like `.github/`, `VS17/`, `appveyor.yml`, test binaries, etc.

## 3. Fix `readme` path warning for `readstat` crate

`cargo publish --dry-run` warns: *"readme `../../README.md` appears to be a path outside of the package"*. Change `readstat/Cargo.toml` to `readme = "README.md"` (the per-crate README already exists). Same for `readstat-cli/Cargo.toml`.

## 4. Replace `println!` with `log` macros in library code

A library crate should never print directly to stdout. The `readstat` library uses `println!` in several places:

- `rs_write_config.rs:112` — "Ignoring value of --compression-level..."
- `rs_write_config.rs:188` — "The file {} will be overwritten!"
- `rs_write_config.rs:229` — "Compression level is not required..."
- `rs_write.rs:323` — "In total, wrote N rows..."
- `rs_write.rs:643` — metadata stdout output (this one is intentional for the `write_metadata_to_stdout` method)

Replace the warning/info messages with `log::warn!` / `log::info!`. The "wrote N rows" message should use `log::info!`. The metadata stdout output is intentional and can stay.

## 5. Fix clippy `doc_markdown` warnings

Backtick `ReadStat` in doc comments across all crates. ~43 instances total, concentrated in:
- `iconv-sys/src/lib.rs` (1)
- `readstat-sys/src/lib.rs` (2)
- Various doc comments in the `readstat` crate

Run `cargo clippy --fix` for the simple cases, then manually review.

## 6. Rename `OutFormat` variants to PascalCase

The `OutFormat` enum uses lowercase variants (`csv`, `feather`, `ndjson`, `parquet`) with `#[allow(non_camel_case_types)]`. Before publishing the first version to crates.io, rename to idiomatic PascalCase (`Csv`, `Feather`, `Ndjson`, `Parquet`) and remove the allow attribute. The `Display` impl already exists and can map to lowercase strings. Update all match arms in the library and CLI.

## 7. Add `#[must_use]` attributes

Add `#[must_use]` to builder-pattern methods that return `Self` on `ReadStatData` (about 9 methods like `set_no_progress`, `set_total_rows_to_process`, `init`, `init_shared`, etc.). These are methods where ignoring the return value is almost certainly a bug.

## 8. Add `documentation` URL to `readstat-cli` Cargo.toml

The `readstat-cli` crate is missing the `documentation` field. Add `documentation = "https://docs.rs/readstat-cli"`.

## 9. Update ARCHITECTURE.md

- Test module count says "30" but recent additions may have changed this — verify and update
- Update the test count reference in README.md (says "29 modules, 13 datasets" in workspace crates table)
- Verify version numbers in crate descriptions match current versions (0.19.0 / 0.3.0)

## 10. Update CHANGELOG.md

Add the iconv-sys rename and all the cleanup changes from this PR to the `[0.19.0]` section.

## 11. Run full test suite and clippy

After all changes:
```bash
cargo clippy --workspace
cargo test --workspace
cargo doc --workspace --no-deps
cargo publish -p readstat-iconv-sys --dry-run
cargo publish -p readstat-sys --dry-run
cargo publish -p readstat --dry-run
cargo publish -p readstat-cli --dry-run
```

## 12. Publish order

Crates must be published in dependency order, waiting for each to appear on the index:
1. `readstat-iconv-sys` (no crate dependencies)
2. `readstat-sys` (depends on `readstat-iconv-sys`)
3. `readstat` (depends on `readstat-sys`)
4. `readstat-cli` (depends on `readstat`, `readstat-sys`)

---

## Items NOT included (and why)

- **Pedantic clippy fixes** (casting warnings, `too_many_lines`, etc.): These are in FFI/callback code where casts are unavoidable or in auto-generated bindings. Adding `#[allow]` annotations would add noise without value. The default clippy level is already clean.
- **Refactoring `indicatif` out of the library**: The progress bar on `ReadStatData` is a design choice that works. Changing it would be a large refactor for marginal benefit.
- **Changing `c_int` to native types in public API**: Would be a breaking change affecting every user of `ReadStatMetadata`. Can be done in a future major version.
- **Restructuring `ReadStatVarMetadata::new` (8 params)**: Already has `#[allow(clippy::too_many_arguments)]`. A builder pattern would be better but is out of scope for this cleanup.
