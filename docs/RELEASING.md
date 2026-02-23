# Releasing to crates.io

Step-by-step guide for publishing readstat-rs crates to crates.io.

## Quick Reference

```bash
# Run all pre-publish checks
./scripts/release-check.sh        # Linux/macOS
.\scripts\release-check.ps1       # Windows

# Switch vendor dirs from submodules to copied files
./scripts/vendor.sh prepare       # Linux/macOS
.\scripts\vendor.ps1 prepare      # Windows

# Publish (in dependency order)
cargo publish -p readstat-iconv-sys
cargo publish -p readstat-sys
cargo publish -p readstat
cargo publish -p readstat-cli

# Restore submodules after publishing
./scripts/vendor.sh restore       # Linux/macOS
.\scripts\vendor.ps1 restore      # Windows
```

---

## Pre-Release Checklist

### 1. Version Bumps

Update version numbers in these files (keep them in sync):

| File | Fields |
|------|--------|
| `crates/readstat/Cargo.toml` | `version` |
| `crates/readstat-cli/Cargo.toml` | `version`, `readstat` dependency version |
| `crates/readstat-sys/Cargo.toml` | `version` |
| `crates/iconv-sys/Cargo.toml` | `version` |
| `crates/readstat/Cargo.toml` | `readstat-sys` dependency version |
| `crates/readstat-sys/Cargo.toml` | `readstat-iconv-sys` dependency version |

**Version conventions:**
- `readstat` and `readstat-cli` share the same version (e.g. `0.19.0`)
- `readstat-sys` and `readstat-iconv-sys` share the same version (e.g. `0.3.0`)
- Bump sys crate versions only when the vendored C library or bindings change

### 2. Update CHANGELOG.md

Add an entry for the new version:
```markdown
## [0.20.0] - 2026-XX-XX

### Added
- ...

### Changed
- ...

### Fixed
- ...
```

### 3. Run Automated Checks

```bash
./scripts/release-check.sh
```

This runs:
- `cargo fmt --all -- --check` — formatting
- `cargo clippy --workspace` — linting
- `cargo test --workspace` — all tests
- `cargo doc --workspace --no-deps` — documentation build
- `cargo deny check` — license and security audit (if installed)
- Version consistency checks
- CHANGELOG entry check
- `cargo package` dry-run for each publishable crate

Fix any failures before proceeding.

### 4. Manual Checks

- [ ] README.md is up to date
- [ ] Documentation reflects any API changes
- [ ] Architecture docs (`docs/ARCHITECTURE.md`) are current
- [ ] mdbook builds cleanly: `./scripts/build-book.sh`

---

## Vendor Preparation

The `readstat-sys` and `readstat-iconv-sys` crates vendor C source code from git
submodules. `cargo publish` cannot include git submodule contents, so the files
must be copied as regular files before publishing.

### Switch to publish mode

```bash
./scripts/vendor.sh prepare       # Linux/macOS
.\scripts\vendor.ps1 prepare      # Windows
```

This:
1. Records submodule commit hashes in `vendor-lock.txt`
2. Copies only the files needed for building (matching `Cargo.toml` `include` patterns)
3. Deinitializes the git submodules
4. Places the copied files in the vendor directories

### Verify package contents

```bash
cargo package --list -p readstat-sys --allow-dirty
cargo package --list -p readstat-iconv-sys --allow-dirty
```

---

## Publishing

Crates must be published in dependency order. Wait for each crate to appear on
the crates.io index before publishing the next one.

```bash
# 1. No crate dependencies
cargo publish -p readstat-iconv-sys

# 2. Depends on readstat-iconv-sys (Windows only)
cargo publish -p readstat-sys

# 3. Depends on readstat-sys
cargo publish -p readstat

# 4. Depends on readstat
cargo publish -p readstat-cli
```

**Note:** There may be a delay (30 seconds to a few minutes) between publishing
a crate and it appearing in the index. If `cargo publish` fails with a dependency
resolution error, wait and retry.

---

## Post-Publish

### 1. Restore submodules

```bash
./scripts/vendor.sh restore       # Linux/macOS
.\scripts\vendor.ps1 restore      # Windows
```

### 2. Create a git tag

```bash
git tag v0.20.0
git push origin v0.20.0
```

### 3. Create a GitHub release

Use the GitHub CLI or web UI to create a release from the tag. The CI pipeline
(`main.yml`) will automatically build platform binaries and attach them.

### 4. Clean up

- Remove `vendor-lock.txt` (or commit it for reference)
- Verify the published crates on [crates.io](https://crates.io)
- Verify the docs on [docs.rs](https://docs.rs)

---

## Troubleshooting

### `cargo publish` fails with "no matching package found"

The dependency crate hasn't appeared in the index yet. Wait 30-60 seconds and retry.

### `cargo package` includes too many files

Check the `include` field in the crate's `Cargo.toml`. Run `cargo package --list`
to see exactly what will be included.

### Vendor files missing after `vendor.sh restore`

Run `git submodule update --init --recursive` to re-initialize.

### Build fails after switching vendor modes

Clean the build cache: `cargo clean` then rebuild.
