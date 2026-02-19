[< Back to README](../README.md)

# Technical Details

## Floating Point Values
:warning: Decimal values are rounded to contain only 14 decimal digits!

For example, the number `1.1234567890123456` created within SAS would be returned as `1.12345678901235` within Rust.

Why does this happen?  Is this an implementation error?  No, rounding to only 14 decimal digits has been _purposely implemented_ within the Rust code.

As a specific example, when testing with the [cars.sas7bdat](../crates/readstat-tests/tests/data/README.md) dataset (which was created originally on Windows), the numeric value `4.6` as observed within SAS was being returned as `4.600000000000001` (15 digits) within Rust.  Values created on Windows with an x64 processor are only accurate to 15 digits.

For comparison, the [ReadStat binary](https://github.com/WizardMac/ReadStat#command-line-usage) [truncates to 14 decimal places](https://github.com/WizardMac/ReadStat/blob/master/src/bin/write/mod_csv.c#L147) when writing to `csv`.

Finally, SAS represents all numeric values in floating-point representation which creates a challenge for **all** parsed numerics!

### Implementation: pure-arithmetic rounding

Rounding is performed using pure f64 arithmetic in `cb.rs`, avoiding any string formatting or heap allocation:

```rust
const ROUND_SCALE: f64 = 1e14;

fn round_decimal_f64(v: f64) -> f64 {
    if !v.is_finite() { return v; }
    let int_part = v.trunc();
    let frac_part = v.fract();
    let rounded_frac = (frac_part * ROUND_SCALE).round() / ROUND_SCALE;
    int_part + rounded_frac
}
```

The value is split into integer and fractional parts before scaling. This is necessary because large SAS datetime values (~1.9e9) multiplied directly by 1e14 would exceed f64's exact integer range (2^53), causing precision loss. Since `fract()` is always in (-1, 1), `fract() * 1e14 < 1e14 < 2^53`, keeping the scaled value within the exact-integer range.

**Why this is equivalent to the previous string roundtrip** (`format!("{:.14}")` + `lexical::parse`): both approaches produce the nearest representable f64 to the value rounded to 14 decimal places. The tie-breaking rule (half-away-from-zero for `.round()` vs half-to-even for `format!`) is never exercised because every f64 is a dyadic rational (m / 2^k), and a true decimal midpoint would require an odd factor of 5 in the denominator — which is impossible for any f64 value.

### Sources
- [How SAS Stores Numeric Values](https://documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=9.4_3.5&docsetId=lrcon&docsetTarget=p0ji1unv6thm0dn1gp4t01a1u0g6.htm&locale=en#n00dmtao82eizen1e6yziw3s31da)
- [Accuracy on x64 Windows Processors](https://documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=9.4_3.5&docsetId=lrcon&docsetTarget=p0ji1unv6thm0dn1gp4t01a1u0g6.htm&locale=en#n0pd8l179ai8odn17nncb4izqq3d)
    - SAS on Windows with x64 processors can only represent 15 digits
- [Floating-point arithmetic may give inaccurate results in Excel](https://docs.microsoft.com/en-us/office/troubleshoot/excel/floating-point-arithmetic-inaccurate-result)
- [What Every Computer Scientist Should Know About Floating-Point Arithmetic (Goldberg, 1991)](https://docs.oracle.com/cd/E19957-01/806-3568/ncg_goldberg.html)

## Date, Time, and Datetimes
All 118 SAS date, time, and datetime formats are recognized and parsed appropriately.  For the full list of supported formats, see [sas_date_time_formats.md](../crates/readstat-tests/util/sas_date_time_formats.md).

:warning: If the format does not match a recognized SAS date, time, or datetime format, or if the value does not have a format applied, then the value will be parsed and read as a numeric value!

### Details
SAS stores [dates, times, and datetimes](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/lrcon/p1wj0wt2ebe2a0n1lv4lem9hdc0v.htm) internally as numeric values.  To distinguish among dates, times, datetimes, or numeric values, a SAS format is read from the variable metadata.  If the format matches a recognized SAS date, time, or datetime format then the numeric value is converted and read into memory using one of the Arrow types:
- [Date32Type](https://docs.rs/arrow/latest/arrow/datatypes/struct.Date32Type.html)
- [Time32SecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.Time32SecondType.html)
- [Time64MicrosecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.Time64MicrosecondType.html) &mdash; for time formats with microsecond precision (e.g. `TIME15.6`, decimal places 4&ndash;6)
- [TimestampSecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.TimestampSecondType.html)
- [TimestampMillisecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.TimestampMillisecondType.html) &mdash; for datetime formats with millisecond precision (e.g. `DATETIME22.3`, decimal places 1&ndash;3)
- [TimestampMicrosecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.TimestampMicrosecondType.html) &mdash; for datetime formats with microsecond precision (e.g. `DATETIME22.6`, decimal places 4&ndash;6)

If values are read into memory as Arrow date, time, or datetime types, then when they are written &mdash; from an Arrow [`RecordBatch`](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) to `csv`, `feather`, `ndjson`, or `parquet` &mdash; they are treated as dates, times, or datetimes and not as numeric values.

## Column Metadata in Arrow and Parquet

When converting to Parquet or Feather, readstat-rs persists column-level and table-level metadata into the Arrow schema. This metadata survives round-trips through Parquet and Feather files, allowing downstream consumers to recover SAS-specific information.

### Metadata keys

#### Field (column) metadata

| Key | Type | Description | Source formats |
|-----|------|-------------|----------------|
| `label` | string | User-assigned variable label | SAS, SPSS, Stata |
| `sas_format` | string | SAS format string (e.g. `DATE9`, `BEST12`, `$30`) | SAS |
| `storage_width` | integer (as string) | Number of bytes used to store the variable value | All |
| `display_width` | integer (as string) | Display width hint from the file | XPORT, SPSS |

#### Schema (table) metadata

| Key | Type | Description |
|-----|------|-------------|
| `table_label` | string | User-assigned file label |

### Storage width semantics

- **SAS numeric variables**: always 8 bytes (IEEE 754 double-precision)
- **SAS string variables**: equal to the declared character length (e.g. `$30` → 30 bytes)
- The `storage_width` field is always present in metadata

### Display width semantics

- **sas7bdat files**: typically 0 (not stored in the format)
- **XPORT files**: populated from the format width
- **SPSS files**: populated from the variable's print/write format
- The `display_width` field is only present in metadata when non-zero

### SAS format strings and Arrow types

The SAS format string (e.g. `DATE9`, `DATETIME22.3`, `TIME8`) determines how a numeric variable is mapped to an Arrow type. The original format string is preserved in the `sas_format` metadata key, allowing downstream tools to reconstruct the original SAS formatting even after conversion.

For the full list of recognized SAS date, time, and datetime formats, see [sas_date_time_formats.md](../crates/readstat-tests/util/sas_date_time_formats.md).

### Reading metadata from output files

See the [Reading Metadata from Output Files](USAGE.md#reading-metadata-from-output-files) section in the Usage guide for Python and R examples.
