# readstat-sys

Raw FFI bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library, generated with [bindgen](https://rust-lang.github.io/rust-bindgen/).

The `build.rs` script compiles ~49 C source files from the vendored `vendor/ReadStat/` git submodule via the `cc` crate and generates Rust bindings with `bindgen`. Platform-specific linking for iconv and zlib is handled automatically (see [docs/BUILDING.md](../../docs/BUILDING.md) for details).

These bindings expose the **full** ReadStat API, including support for SAS (`.sas7bdat`, `.xpt`), SPSS (`.sav`, `.zsav`, `.por`), and Stata (`.dta`) file formats.

This is a [sys crate](https://kornel.ski/rust-sys-crate) — it exposes raw C types and functions. Use the [`readstat`](https://crates.io/crates/readstat) library crate for a safe, high-level API (currently SAS `.sas7bdat` only).

## API Coverage

All 125 public C functions and all 8 enum types from `readstat.h` are bound. All 49 library source files are compiled.

### Functions by Category

| Category | Count | Formats |
|----------|------:|---------|
| **Metadata accessors** | 15 | All |
| **Value accessors** | 14 | All |
| **Variable accessors** | 14 | All |
| **Parser lifecycle** | 3 | All |
| **Parser callbacks** | 7 | All |
| **Parser I/O handlers** | 6 | All |
| **Parser config** | 4 | All |
| **File parsers (readers)** | 10 | SAS (`sas7bdat`, `sas7bcat`, `xport`), SPSS (`sav`, `por`), Stata (`dta`), text schema (`sas_commands`, `spss_commands`, `stata_dictionary`, `txt`) |
| **Schema parsing** | 1 | All |
| **Writer lifecycle** | 3 | All |
| **Writer label sets** | 5 | All |
| **Writer variable definition** | 11 | All |
| **Writer notes/strings** | 3 | All |
| **Writer metadata setters** | 8 | All |
| **Writer begin** | 6 | SAS (`sas7bdat`, `sas7bcat`, `xport`), SPSS (`sav`, `por`), Stata (`dta`) |
| **Writer validation** | 2 | All |
| **Writer row insertion** | 12 | All |
| **Error handling** | 1 | All |
| **Total** | **125** | |

### Compiled Source Files

| Directory | Files | Description |
|-----------|------:|-------------|
| `src/` (core) | 11 | Hash table, parser, value/variable handling, writer, I/O, error |
| `src/sas/` | 11 | SAS7BDAT, SAS7BCAT, XPORT read/write, IEEE float, RLE compression |
| `src/spss/` | 16 | SAV, POR, ZSAV read/write, compression, SPSS parsing |
| `src/stata/` | 4 | DTA read/write, timestamp parsing |
| `src/txt/` | 7 | SAS commands, SPSS commands, Stata dictionary, plain text, schema |
| **Total** | **49** | |

### Enum Types

| C Enum | Rust Type Alias | Description |
|--------|-----------------|-------------|
| `readstat_type_e` | `readstat_type_e` | Data types (string, int8/16/32, float, double, string_ref) |
| `readstat_type_class_e` | `readstat_type_class_e` | Type classes (string, numeric) |
| `readstat_measure_e` | `readstat_measure_e` | Measurement levels (nominal, ordinal, scale) |
| `readstat_alignment_e` | `readstat_alignment_e` | Column alignment (left, center, right) |
| `readstat_compress_e` | `readstat_compress_e` | Compression types (none, rows, binary) |
| `readstat_endian_e` | `readstat_endian_e` | Byte order (big, little) |
| `readstat_error_e` | `readstat_error_e` | Error codes (41 variants) |
| `readstat_io_flags_e` | `readstat_io_flags_e` | I/O flags |

## Verifying Bindings

To confirm that the Rust bindings stay in sync with the vendored C header and source files, run the verification script:

```bash
# Bash (Linux, macOS, Windows Git Bash)
bash crates/readstat-sys/verify_bindings.sh

# Rebuild first, then verify
bash crates/readstat-sys/verify_bindings.sh --rebuild
```

```powershell
# PowerShell (Windows)
.\crates\readstat-sys\verify_bindings.ps1

# Rebuild first, then verify
.\crates\readstat-sys\verify_bindings.ps1 -Rebuild
```

The script checks three things:
1. Every function declared in `readstat.h` has a `pub fn` binding in the generated `bindings.rs`
2. Every `typedef enum` in the header has a corresponding Rust type alias
3. Every `.c` library source file in the vendor directory is listed in `build.rs`

Run this after updating the ReadStat submodule to catch any new or removed API surface.
