[< Back to README](../README.md)

# GitHub Actions

The CI/CD workflow can be triggered in multiple ways:

## 1. Tag Push (Release)

Push a tag to trigger a full release build with GitHub Release artifacts:

```sh
# add and commit local changes
git add .
git commit -m "commit msg"

# push local changes to remote
git push

# add local tag
git tag -a v0.1.0 -m "v0.1.0"

# push local tag to remote
git push origin --tags
```

To delete and recreate tags:

```sh
# delete local tag
git tag --delete v0.1.0

# delete remote tag
git push origin --delete v0.1.0
```

## 2. Manual Trigger (GitHub UI)

Trigger a build manually from the GitHub Actions web interface (build-only, no releases):

1. Go to the [Actions tab](https://github.com/curtisalexander/readstat-rs/actions)
2. Select the **readstat-rs** workflow
3. Click **Run workflow**
4. Optionally specify:
   - **Version string**: Label for artifacts (default: `dev`)

:memo: Manual triggers only build artifacts and do not create GitHub releases. To create a release, use a [tag push](#1-tag-push-release).

## 3. API Trigger (External Tools)

Trigger builds programmatically using the GitHub API. This is useful for automation tools like Claude Code.

### Using `gh` CLI

```sh
# Trigger a build
gh api repos/curtisalexander/readstat-rs/dispatches \
  -f event_type=build

# Trigger a build with custom version label
gh api repos/curtisalexander/readstat-rs/dispatches \
  -f event_type=build \
  -F client_payload='{"version":"test-build-123"}'
```

### Using `curl`

```sh
curl -X POST \
  -H "Authorization: token $GITHUB_TOKEN" \
  -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/repos/curtisalexander/readstat-rs/dispatches \
  -d '{"event_type": "build", "client_payload": {"version": "dev"}}'
```

## 4. Claude Code Integration

To have Claude Code trigger a CI build, use this prompt:

> Trigger a CI build for readstat-rs by running: `gh api repos/curtisalexander/readstat-rs/dispatches -f event_type=build`

## Event Types

Repository dispatch event types for API triggers:

| Event Type | Description |
|------------|-------------|
| `build`    | Build all targets and upload artifacts |
| `test`     | Same as `build` (alias for clarity) |
| `release`  | Same as `build` (reserved for future use) |

:memo: API triggers only build artifacts and do not create GitHub releases. To create a release, use a [tag push](#1-tag-push-release).

## Fuzz Testing (`.github/workflows/fuzz.yml`)

A separate workflow runs [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) (libFuzzer) targets against the `readstat` library's byte-parsing paths.

- **Schedule**: Weekly, Monday 3am UTC
- **Manual trigger**: `gh workflow run fuzz.yml` or via the Actions UI
- **Duration**: 30 minutes per target (~90 min total)
- **Targets**: `fuzz_read_metadata`, `fuzz_read_data`, `fuzz_read_data_filtered`
- **On crash**: uploads crash artifacts and automatically opens a GitHub issue labeled `bug` + `fuzz`

See [TESTING.md](TESTING.md#fuzz-testing) for local usage and target details.

## Artifacts

All builds (regardless of trigger method) upload artifacts that can be downloaded from the workflow run page. Artifacts are retained for the default GitHub Actions retention period.

## readstat-sys Cross-Platform CI (`.github/workflows/readstat-sys-ci.yml`)

A separate workflow guards the FFI bindings. The `readstat-sys` and `readstat-iconv-sys` crates ship **checked-in, per-target pre-generated bindings** so that downstream builds need no `libclang`. This workflow proves those files are correct and reproducible. It runs on PRs / pushes touching `crates/readstat-sys/**`, `crates/readstat-iconv-sys/**`, `Cargo.toml`, `Cargo.lock`, or the workflow file, and supports `workflow_dispatch`.

Three jobs:

| Job | Runs on | What it does |
|-----|---------|--------------|
| `consume` | linux x86_64/aarch64, macOS x86_64/aarch64, windows x86_64 | Builds + tests the workspace using the **committed** `bindings_<os>_<arch>.rs` — the load-bearing check that each file matches that platform's real ABI. |
| `regen` | same matrix | Regenerates each target's bindings with `--features buildtime_bindgen`, uploads the result as artifact `bindings-<os>`, and **fails on drift** if it differs from the committed file. |
| `regen-iconv` | windows x86_64 | Same idea for `readstat-iconv-sys`; artifact `iconv-bindings-windows`. |

The checked-in files live in:
- `crates/readstat-sys/src/bindings/bindings_<os>_<arch>.rs` (`<os>` ∈ linux/macos/windows, `<arch>` ∈ x86_64/aarch64)
- `crates/readstat-iconv-sys/src/bindings/bindings_windows_x86_64.rs`

### Updating bindgen (or the vendored C) — regenerating bindings

`bindgen` is **exact-pinned** in the workspace `Cargo.toml` (`bindgen = "=x.y.z"`) because its output *is* the checked-in bindings; a different bindgen version can change that output. The exact pin means `cargo update` and `scripts/check-updates.sh` never bump it — it is always a deliberate, manual change, paired with regenerating every target's bindings. The same procedure applies when you bump the vendored `ReadStat` / `libiconv` submodule and its C surface changes.

You can only regenerate **your own host target** locally (cross-compiling the others needs each platform's toolchain + `libclang`, and Windows for iconv). So: verify locally, then let CI regenerate the rest.

**Do locally:**

1. Edit the pin in `Cargo.toml`: `bindgen = "=<new-version>"`.
2. Regenerate + sanity-check your host target (needs `libclang` installed):
   ```sh
   cargo build -p readstat-sys --features buildtime_bindgen
   # On Windows, also:
   cargo build -p readstat-iconv-sys --features buildtime_bindgen
   ```
   The build script writes the regenerated file to both `OUT_DIR` and the checked-in
   `src/bindings/bindings_<host-os>_<host-arch>.rs`, so it shows up as a working-tree change.
3. Confirm it still works: `cargo test --workspace`.
4. Commit the `Cargo.toml` change together with your host target's regenerated file. (The other targets will still be stale — that's expected; CI produces them next.)

**Let CI do (the targets you can't build locally):**

5. Push the branch. The `regen` matrix (5 targets) and `regen-iconv` run on real runners with `libclang`. For every target whose committed file you didn't refresh, the **drift check fails on purpose** — that failure is the signal, and each job still uploads its freshly-generated file as an artifact (`bindings-<os>`, `iconv-bindings-windows`).
6. Download those artifacts, drop them into the two `src/bindings/` directories above, commit, and push.
7. On the next run the `regen` / `regen-iconv` drift checks pass and the `consume` jobs build + test green on all platforms. The bindgen bump is complete.

> :bulb: You can cut the regenerated files on demand without a PR via **Actions → readstat-sys cross-platform CI → Run workflow** (`workflow_dispatch`), then grab the artifacts.

`scripts/check-updates.sh` (and `.ps1`) print an advisory pointing here whenever a newer `bindgen` than the pin is available.
