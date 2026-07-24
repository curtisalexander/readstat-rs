[< Back to README](../README.md)

# GitHub Actions lifecycle

The automation is split by lifecycle so fast validation, long-running safety work,
and publication have clear ownership.

## CI (`.github/workflows/ci.yml`)

CI runs on pull requests and pushes to `main`/`dev`, manually, and as a reusable
workflow. Four independent gates start in parallel:

| Gate | Purpose |
|---|---|
| `verify` | Formatting, non-SQL feature combinations, core tests, book, host WASM lint, package contents, and Arrow/DataFusion lockstep. |
| `sql` | All-feature clippy, workspace tests, rustdoc, and advertised API examples. |
| `wasm` | Emscripten release build and Node metadata smoke test. |
| `msrv` | Workspace/default and readstat/CLI all-feature checks on Rust 1.88. |

PR and branch runs cancel superseded work. Release calls use a unique run/run-attempt
concurrency key, never cancel, and run MSRV as well. `RUSTFLAGS` is job-local so
Emscripten receives its own required configuration.

## Safety (`.github/workflows/safety.yml`)

Safety runs weekly (Wednesday at 04:17 UTC), manually, and as a reusable workflow.
It runs Miri plus Linux, macOS, and Windows AddressSanitizer checks. Ordinary Windows
Rust ASan is blocking. The broader ReadStat-C-and-Rust Windows instrumentation is
explicitly experimental, `continue-on-error` telemetry. Safety runs are never
canceled and are required by release assembly.

## Releases (`.github/workflows/release.yml`)

Strict `vN.N.N` tag pushes may publish. Manual runs (safe label defaults to `dev`)
and repository-dispatch `build`, `test`, and `release` events are build-only dry
runs. Preparation rejects malformed tags/labels, package-version mismatches, and
tagged commits not contained in `origin/main`.

After preparation, CI, safety, and seven candidate builds run concurrently: Linux
GNU x86_64, Linux musl x86_64, Linux GNU ARM64, macOS x86_64/ARM64, and Windows
MSVC/GNU. Candidates only upload `candidate-*` workflow artifacts. A single final
job downloads those artifacts, verifies the exact seven archive names, creates
`SHA256SUMS`, and uploads the assembled bundle on every trigger. Only on a strict
tag push does that final job check that no release already exists, generate notes
from strict reachable version tags, and publish once. Thus a failed platform or
safety check cannot leave a partially published GitHub Release.

To run a dry build in the UI select **Release candidates**, or use:

```sh
gh workflow run release.yml -f version=dev
gh api repos/curtisalexander/readstat-rs/dispatches -f event_type=build \
  -F client_payload='{"version":"test-build-123"}'
```

API event types `build`, `test`, and `release` are aliases and never publish.

## Bindings (`.github/workflows/readstat-sys-ci.yml`)

This workflow runs monthly (day 1 at 05:23 UTC), manually, and for relevant PRs or
`main`/`dev` pushes. Six consume jobs immediately build/test committed bindings on
Linux x86/ARM, macOS x86/ARM, and Windows MSVC/GNU. An independent detector uses
event SHAs to decide whether ReadStat's six-target regeneration matrix and/or the
Windows iconv regeneration is needed; uncertainty fails open and regenerates both.
Regeneration uploads bindings before enforcing tracked-file and drift checks. Only
superseded PR runs are canceled.

To refresh bindings, run the workflow (or push a sensitive change), download each
`bindings-<target>` / `iconv-bindings-windows` artifact from an intentionally failed
drift job, commit the files under the crates' `src/bindings/` directories, and rerun.
`READSTAT_REGEN_BINDINGS=1 cargo build -p <sys-crate> --features buildtime_bindgen`
performs the equivalent operation for the native host.

## Fuzzing and Pages

`fuzz.yml` runs three parallel cargo-fuzz campaigns every Monday at 03:00 UTC or
manually. Each campaign lasts 15 minutes; crashes upload artifacts and open an issue.
Each invocation has unique non-canceling concurrency. `pages.yml` remains separate
and deploys the mdBook on `main` pushes or manual dispatch.
