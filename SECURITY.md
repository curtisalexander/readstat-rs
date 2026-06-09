# Security Policy

## Supported Versions

`readstat-rs` is pre-1.0. Security fixes are applied to the latest published
version of each crate (`readstat`, `readstat-cli`, `readstat-sys`,
`readstat-iconv-sys`). Older versions are not maintained — please upgrade to
the latest release before reporting an issue.

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Instead, report privately using GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository (the **Security** tab → **Report a vulnerability**).

When reporting, please include:

- The affected crate and version.
- A description of the vulnerability and its impact.
- Steps to reproduce, ideally with a minimal `.sas7bdat` sample or test case.

You can expect an initial acknowledgement within a few days. Once a fix is
available, a patched release will be published to crates.io and a GitHub
Security Advisory issued.

## Scope and Context

`readstat-rs` parses **untrusted binary input** (`.sas7bdat` files) through FFI
bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library.
Memory-safety is therefore a primary concern. The project runs automated
memory-safety checks in CI (Valgrind, AddressSanitizer, Miri where applicable,
and an unsafe-code audit) and maintains a fuzzing harness — see
[docs/MEMORY-SAFETY.md](docs/MEMORY-SAFETY.md) and
[docs/TESTING.md](docs/TESTING.md#fuzz-testing).

Vulnerabilities that originate in the upstream ReadStat C library may be
forwarded to the [ReadStat project](https://github.com/WizardMac/ReadStat) in
addition to being addressed here where feasible.
